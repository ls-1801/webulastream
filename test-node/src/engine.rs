use nes_network::protocol::TupleBuffer;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use tracing::{error, info};

type ComputeFn = Box<dyn Fn(TupleBuffer) -> TupleBuffer + Sync + Send>;
pub(crate) type EmitFn = Box<dyn Fn(TupleBuffer) + Sync + Send>;
type Queue = crossbeam_queue::ArrayQueue<Task>;
struct SimplePipeline {
    pub fun: ComputeFn,
}

pub struct Node {
    pipeline: Arc<dyn ExecutablePipeline + Send + Sync>,
    successor: Option<Arc<Node>>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.pipeline.stop();
    }
}

impl Node {
    pub fn new(
        successor: Option<Arc<Node>>,
        pipeline: Arc<dyn ExecutablePipeline + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pipeline,
            successor,
        })
    }
}

pub struct SourceNode {
    implementation: Box<dyn SourceImpl + Send + Sync>,
    successor: Arc<Node>,
}

impl SourceNode {
    fn start(&self, queue: Arc<Queue>, query_id: usize) {
        let successor = self.successor.clone();
        let emit_fn = Box::new(move |buffer| {
            queue.push(Task::Compute(query_id, buffer, successor.clone()));
        }) as EmitFn;
        self.implementation.start(emit_fn);
    }
    fn stop(&self) {
        self.implementation.stop()
    }
}

impl SourceNode {
    pub fn new(
        successor: Arc<Node>,
        implementation: Box<dyn SourceImpl + Send + Sync>,
    ) -> SourceNode {
        SourceNode {
            successor,
            implementation,
        }
    }
}

pub trait SourceImpl {
    fn start(&self, emit: EmitFn);
    fn stop(&self);
}
pub trait PipelineContext {
    fn emit(&mut self, data: TupleBuffer);
}
pub trait ExecutablePipeline {
    fn execute(&self, data: &TupleBuffer, context: &mut dyn PipelineContext);
    fn stop(&self);
}

enum Task {
    Compute(usize, TupleBuffer, Arc<Node>),
}

pub struct Query {
    sources: Vec<Arc<Mutex<SourceNode>>>,
}

impl Query {
    pub fn new(sources: Vec<SourceNode>) -> Self {
        let sources = sources
            .into_iter()
            .map(|node| Arc::new(Mutex::new(node)))
            .collect();
        Self { sources }
    }
}

pub struct QueryEngine {
    id_counter: AtomicUsize,
    queue: Arc<Queue>,
    queries: Mutex<HashMap<usize, Query>>,
}

impl Default for QueryEngine {
    fn default() -> Self {
        QueryEngine {
            queue: Arc::new(crossbeam_queue::ArrayQueue::new(1024)),
            queries: Mutex::default(),
            id_counter: AtomicUsize::default(),
        }
    }
}

struct PEC<'a> {
    query_id: usize,
    queue: &'a Queue,
    successor: &'a Option<Arc<Node>>,
}

impl PipelineContext for PEC<'_> {
    fn emit(&mut self, data: TupleBuffer) {
        if let Some(successor) = self.successor {
            self.queue
                .push(Task::Compute(self.query_id, data, successor.clone()));
        }
    }
}

impl QueryEngine {
    pub(crate) fn start() -> Arc<QueryEngine> {
        let engine = Arc::new(QueryEngine::default());
        let pool = ThreadPool::with_name("engine".to_string(), 2);

        pool.execute({
            let engine = engine.clone();
            move || loop {
                if let Some(task) = engine.queue.pop() {
                    match task {
                        Task::Compute(query_id, input, node) => {
                            let mut pec = PEC {
                                query_id,
                                queue: &engine.queue,
                                successor: &node.successor,
                            };
                            let span = tracing::info_span!("Task", query_id = query_id);
                            {
                                let _enter = span.enter();
                                node.pipeline.execute(&input, &mut pec);
                            }
                        }
                    }
                }
            }
        });

        engine
    }

    pub fn stop_query(self: &Arc<Self>, id: usize) {
        if let Some(query) = self.queries.lock().unwrap().remove(&id) {
            for source in &query.sources {
                source.lock().unwrap().stop();
            }
            info!("Stopped Query with Id {id}");
        } else {
            error!("Query with id {id} does not exist!");
        }
    }
    pub fn start_query(self: &Arc<Self>, query: Query) -> usize {
        let id = self.id_counter.fetch_add(1, Relaxed);
        for source in &query.sources {
            source.lock().unwrap().start(self.queue.clone(), id);
        }
        self.queries.lock().unwrap().insert(id, query);
        info!("Started Query with id {id}");
        id
    }
}
