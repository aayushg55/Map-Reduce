//! The MapReduce coordinator.
//!

use anyhow::Result;
use itertools::Itertools;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Code};

use crate::rpc::coordinator::*;
use crate::*;
use crate::{log};
use std::sync::Arc;
use std::usize;
use tokio::sync::Mutex;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::{SystemTime, Duration};
pub mod args;

pub struct Job {
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_map: u32,
    n_reduce: u32,
    args: Vec<u8>,

    to_do_map: VecDeque<u32>,
    to_do_reduce: VecDeque<u32>,
    num_map_finished: u32,
    num_reduce_finished: u32,
    map_task_assignments: Vec<MapTaskAssignment>,

    errors: Vec<String>,
    is_clean: bool,
}

impl Job {
    pub fn new(files: Vec<String>, output_dir: String, app: String, n_reduce:u32, args:Vec<u8>) -> Self {
        let n_map: u32 = files.len() as u32;
        Self {
            files,
            output_dir,
            app,
            n_map,
            n_reduce,
            args,
        
            to_do_map: (0..n_map).collect(),
            to_do_reduce: (0..n_reduce).collect(),
            num_map_finished: 0,
            num_reduce_finished: 0,
            map_task_assignments: vec![MapTaskAssignment{task: 0, worker_id: 0}; n_map as usize],
            errors: Vec::new(),
            is_clean: false,
        }
    }
    pub fn is_finished(&self) -> bool {
        self.num_reduce_finished == self.n_reduce
    }
    pub fn finished_map(&self) -> bool {
        self.num_map_finished == self.n_map
    }
    pub fn errored(&self) -> bool {
        self.errors.len() > 0
    }
    pub fn get_status(&self) -> Result<Response<PollJobReply>, Status>{
        let reply = PollJobReply {
            done: self.is_finished(), 
            failed: self.errored(), 
            errors: self.errors.clone(),
        };
        log::info!("Status is done? {d},  failed? {f}. Finished {r} reduce and {m} map tasks.", d=reply.done, f=reply.failed, r=self.num_reduce_finished, m=self.num_map_finished);
        Ok(Response::new(reply))
    }
    pub fn add_map_worker(&mut self, task_num: u32, worker_id: u32) {
        log::info!("Adding worker {worker_id} to map task {task_num}. Total {total} map tasks.", total=self.map_task_assignments.len());

        self.map_task_assignments[task_num as usize] = MapTaskAssignment { task: task_num, worker_id }
    }

    pub fn clean(&mut self) {
        if !self.is_clean {
            self.map_task_assignments.clear();
            self.to_do_map.clear();
            self.to_do_reduce.clear();
            self.files.clear();
            self.args.clear();
            self.is_clean = true;
        }
    }

    pub fn finish_task(&mut self, worker_id: u32, job_id: u32, task: u32, reduce: bool) {
        if reduce {
            self.num_reduce_finished += 1;
        } else {
            self.num_map_finished += 1;
        }
        if self.is_finished() {
            self.clean();
        }
    }
    pub fn remove_assignment(&mut self, task_num: u32, assignment: JobAssignment, reduce: bool) {
        log::info!(" task {task_num} is being added back. is finished? {f}. is reduce? {r}!", f=assignment.finished, r=reduce);
        if reduce && !assignment.finished{
            self.to_do_reduce.push_back(task_num);
        } else if !reduce {
            if assignment.finished {
                self.num_map_finished -= 1;
            }
            self.to_do_map.push_back(task_num);
            // change assignments arr?
            // self.map_task_assignments[task_num as usize] = MapTaskAssignment { task: task_num, worker_id }
        }
    }
    pub fn remove_worker(&mut self, worker_id: u32, assignments: HashMap<u32, JobAssignment>, reduce: bool) {
        for (task_num, assignment) in assignments {
            // log::info!("worker {worker_id} getting removed from task {task_num}!");
            self.remove_assignment(task_num, assignment, reduce);
        }
    }

    pub fn fail(&mut self, error: String) {
        self.clean();
        self.errors.push(error);
    }
}

impl GetTaskReply {
    pub fn new_def() -> Self {
        Self {
            job_id: 0,
            output_dir: String::new(),
            app: String::new(),
            task: 0,
            file: String::new(),
            n_reduce: 0,
            n_map: 0,
            reduce: false,
            wait: true,
            map_task_assignments: Vec::new(),
            args: Vec::new()
        }
    }
    pub fn new(reduce: bool, job_id: u32, job: &Job, task: u32) -> Self {
        let mut f: String = String::new();
        if !reduce {
            f = job.files.get(task as usize).unwrap().to_string();
        }
        Self {
            job_id,
            output_dir: job.output_dir.clone(),
            app: job.app.clone(),
            task,
            file: f,
            n_reduce: job.n_reduce,
            n_map: job.n_map,
            reduce,
            wait: false,
            map_task_assignments: job.map_task_assignments.clone(),
            args: job.args.clone()
        }
    }
}

pub struct JobAssignment {
    task_num: u32,
    finished: bool,
}
pub struct CoordinatorState {
    // Put mutable state here
    sys_time: SystemTime,
    num_workers: u32,
    num_jobs: u32,
    heartbeat_map: HashMap<u32, u64>,
    job_map: HashMap<u32, Job>,
    job_queue: VecDeque<u32>,
    worker_map_assignments: HashMap<u32, HashMap<u32, HashMap<u32, JobAssignment>>>, // worker id to map of all asigments per job id
    worker_reduce_assignments: HashMap<u32, HashMap<u32, HashMap<u32, JobAssignment>>> // worker id to map of all asigments per job id
}

impl CoordinatorState {
    pub fn new() -> Self {
        Self {
            sys_time: SystemTime::now(),
            num_workers: 0,
            num_jobs: 0,
            heartbeat_map: HashMap::new(),
            job_map: HashMap::new(),
            job_queue: VecDeque::new(),
            worker_map_assignments: HashMap::new(),
            worker_reduce_assignments: HashMap::new(),
        }
    }

    pub fn get_job_status(&self, job_id: u32) -> Result<Response<PollJobReply>, Status> {
        if  job_id >= self.num_jobs {
            Err(Status::new(Code::NotFound, "job id is invalid"))
        } else {
            self.job_map[&job_id].get_status()
        }
    }

    pub fn queue_job(&mut self, job: Job) -> u32 {
        let job_id = self.num_jobs;
        self.num_jobs += 1;
        self.job_map.insert(job_id, job);
        self.job_queue.push_back(job_id);
        job_id
    }

    pub fn register_worker(&mut self) -> u32 {
        let id = self.num_workers;
        self.num_workers += 1;
        id
    }

    pub fn assign_task(&mut self, worker_id: u32) ->  GetTaskReply {
        let binding = self.job_queue.clone();
        let queue = binding.iter().enumerate();
        for (index, job_id)  in queue {
            let job = self.job_map.get_mut(job_id).unwrap();
            if job.is_finished() || job.errored() {
                self.job_queue.remove(index);
                continue;
            }
            if job.finished_map() {
                match job.to_do_reduce.pop_front() {
                    Some(task_num) => {
                        let assignment = JobAssignment {task_num, finished: false};
                        self.worker_reduce_assignments.entry(worker_id).or_default()
                            .entry(*job_id).or_default()
                            .insert(task_num, assignment);
                        log::info!("Worker {worker_id} assigned reduce task {task_num} in job {job_id}");
                        return GetTaskReply::new(true, *job_id, self.job_map.get(job_id).unwrap(), task_num);
                    },
                    _ => (),
                }
            } else {
                match job.to_do_map.pop_front() {
                    Some(task_num) => {
                        job.add_map_worker(task_num, worker_id);
                        let assignment = JobAssignment {task_num, finished: false};
                        self.worker_map_assignments.entry(worker_id).or_default()
                            .entry(*job_id).or_default()
                            .insert(task_num, assignment);
                        log::info!("Worker {worker_id} assigned map task {task_num} in job {job_id}");
                        return GetTaskReply::new(false, *job_id, self.job_map.get(job_id).unwrap(), task_num);
                    },
                    _ => (),
                }
            }
            log::info!("No task for worker {worker_id} in job {job_id}. Length of reduce queue {r}. length of map queue {m}", r=job.to_do_reduce.len(), m=job.to_do_map.len());

        }
        
        GetTaskReply::new_def()
    }

    pub fn finish_task(&mut self, worker_id: u32, job_id: u32, task: u32, reduce: bool) {
        let job = self.job_map.get_mut(&job_id).unwrap();
        job.finish_task(worker_id, job_id, task, reduce);
        // let mut assignments = if reduce {self.worker_reduce_assignments} else {self.worker_map_assignments};
        if reduce {
            self.worker_reduce_assignments
                .get_mut(&worker_id).unwrap()
                .get_mut(&job_id).unwrap()
                .entry(task)
                .and_modify(|e| {
                    e.finished = true;
                });
        } else {
            self.worker_map_assignments
                .get_mut(&worker_id).unwrap()
                .get_mut(&job_id).unwrap()
                .entry(task)
                .and_modify(|e| {
                    e.finished = true;
                });
        }
    }

    pub fn reduce_crash(&mut self, worker_id: u32, job_id: u32, task: u32) {
        let assignment = self.worker_reduce_assignments
            .get_mut(&worker_id).unwrap()
            .get_mut(&job_id).unwrap()
            .remove(&task).unwrap();
        self.job_map.get_mut(&job_id).unwrap().remove_assignment(task, assignment,true);
    }

    pub fn check_worker_crasher(&mut self) {
        for (worker_id, last_hb) in self.heartbeat_map.clone() {
            let diff = SystemTime::now().duration_since(self.sys_time).expect("Clock went back in time").as_secs() - last_hb;
            if diff > TASK_TIMEOUT_SECS {
                log::info!("worker {worker_id} crashed!");
                self.heartbeat_map.remove(&worker_id);
                match self.worker_map_assignments.remove(&worker_id) {
                    Some(worker_map_assignment) => {
                        for (job_id, assignments) in worker_map_assignment {
                            log::info!("worker {worker_id} getting removed from map job {job_id}!");
                            self.job_map.get_mut(&job_id).unwrap().remove_worker(worker_id, assignments, false);
                        }
                    },
                    None => (),
                }

                match self.worker_reduce_assignments.remove(&worker_id) {
                    Some(worker_reduce_assignments) => {
                        for (job_id, assignments) in worker_reduce_assignments {
                            log::info!("worker {worker_id} getting removed from map job {job_id}!");
                            self.job_map.get_mut(&job_id).unwrap().remove_worker(worker_id, assignments, true);
                        }
                    },
                    None => (),
                }
            }
        }
    }

    pub fn fail_job(&mut self, job_id: u32, error: String) {
        self.job_map.get_mut(&job_id).unwrap().fail(error);
    }
}
pub struct Coordinator {
    // TODO: add your own fields
    inner: Arc<Mutex<CoordinatorState>>,
}

impl Coordinator {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(CoordinatorState::new())) }
    }
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    /// An example RPC.
    ///
    /// Feel free to delete this.
    /// Make sure to also delete the RPC in `proto/coordinator.proto`.
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        let req = req.get_ref();
        let message = format!("Hello, {}!", req.name);
        Ok(Response::new(ExampleReply { message }))
    }

    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        let SubmitJobRequest {files, output_dir, app, n_reduce, args } = req.into_inner();

        match crate::app::named(&app) {
            Err(e) => {
                log::info!("Received bad job request from client. Application {} not found.", app);
                Err(Status::new(Code::InvalidArgument, e.to_string()))
            },
            Ok(_) => {
                let mut coord_state = self.inner.lock().await;
                let j = Job::new(files, output_dir, app, n_reduce, args);
                let assigned_job_id = coord_state.queue_job(j);
                log::info!("Queued job request from client {id}.", id=assigned_job_id);
                Ok(Response::new(SubmitJobReply {job_id: assigned_job_id}))
            },
        }
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        let coord_state = self.inner.lock().await;
        let PollJobRequest { job_id } = req.into_inner();
        log::info!("Getting job status for job {job_id}");
        coord_state.get_job_status(job_id)
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        // TODO: Worker registration
        let HeartbeatRequest { worker_id } = req.into_inner();
        let mut coord_state = self.inner.lock().await;
        let cur_time = SystemTime::now();
        let diff = cur_time.duration_since(coord_state.sys_time).expect("Clock went back in time").as_secs();
        log::info!("Received heartbeat from worker {id} at time {time}", id=worker_id, time=diff);
        coord_state.heartbeat_map.insert(worker_id, diff);
        Ok(Response::new(HeartbeatReply {}))
    }

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        // TODO: Worker registration
        
        let mut coord_state = self.inner.lock().await;
        let id = coord_state.register_worker();
        log::info!("Received registration req. Assigned to id: {}", id);
        Ok(Response::new(RegisterReply { worker_id: id}))
    }
    
    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        // TODO: Tasks
        let GetTaskRequest { worker_id } = req.into_inner();
        let mut coord_state = self.inner.lock().await;
        coord_state.check_worker_crasher();
        Ok(Response::new(coord_state.assign_task(worker_id)))
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        // TODO: Tasks
        let FinishTaskRequest { worker_id, job_id, task, reduce } = req.into_inner();
        let mut coord_state = self.inner.lock().await;

        let task_type = if reduce { "reduce" } else { "map" };
        log::info!("Worker {worker_id} finished {task_type} task in job {job_id}");

        coord_state.finish_task(worker_id, job_id, task, reduce);
        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        // TODO: Fault tolerance
        let FailTaskRequest { worker_id, job_id, task, reduce, retry, error } = req.into_inner();
        let mut coord_state = self.inner.lock().await;

        if retry {
            coord_state.reduce_crash(worker_id, job_id, task);
        } else {
            coord_state.fail_job(job_id, error);
        }
        Ok(Response::new(FailTaskReply {}))
    }
}

pub async fn start(_args: args::Args) -> Result<()> {
    let addr = COORDINATOR_ADDR.parse().unwrap();

    let coordinator = Coordinator::new();
    let svc = coordinator_server::CoordinatorServer::new(coordinator);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
