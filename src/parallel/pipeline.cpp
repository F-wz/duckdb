#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

class PipelineTask : public ExecutorTask {
	static constexpr const idx_t PARTIAL_CHUNK_COUNT = 50;

public:
	explicit PipelineTask(Pipeline &pipeline_p, shared_ptr<Event> event_p)
	    : ExecutorTask(pipeline_p.executor), pipeline(pipeline_p), event(move(event_p)) {
	}

	Pipeline &pipeline;
	shared_ptr<Event> event;
	unique_ptr<PipelineExecutor> pipeline_executor;

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		if (!pipeline_executor) {
			pipeline_executor = make_unique<PipelineExecutor>(pipeline.GetClientContext(), pipeline);
		}
		if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
			bool finished = pipeline_executor->Execute(PARTIAL_CHUNK_COUNT);
			if (!finished) {
				return TaskExecutionResult::TASK_NOT_FINISHED;
			}
		} else {
			pipeline_executor->Execute();
		}
		event->FinishTask();
		pipeline_executor.reset();
		return TaskExecutionResult::TASK_FINISHED;
	}
};

Pipeline::Pipeline(Executor &executor_p)
    : executor(executor_p), ready(false), initialized(false), source_(nullptr), sink_(nullptr) {
}

ClientContext &Pipeline::GetClientContext() {
	return executor.context;
}

bool Pipeline::GetProgress(double &current_percentage, idx_t &source_cardinality) {
	D_ASSERT(this->source_);
	source_cardinality = this->source_->estimated_cardinality;
	if (!initialized) {
		current_percentage = 0;
		return true;
	}
	auto &client = executor.context;
	current_percentage = this->source_->GetProgress(client, *source_state);
	return current_percentage >= 0;
}

void Pipeline::ScheduleSequentialTask(shared_ptr<Event> &event) {
	vector<unique_ptr<Task>> tasks;
	tasks.push_back(make_unique<PipelineTask>(*this, event));
	event->SetTasks(move(tasks));
}

bool Pipeline::ScheduleParallel(shared_ptr<Event> &event) {
	// check if the sink, source and all intermediate operators support parallelism
	if (!this->sink_->ParallelSink()) {
		return false;
	}
	if (!this->source_->ParallelSource()) {
		return false;
	}
	for (auto &op : this->operators_) {
		if (!op->ParallelOperator()) {
			return false;
		}
	}
	if (this->sink_->RequiresBatchIndex()) {
		if (!this->source_->SupportsBatchIndex()) {
			throw InternalException(
			    "Attempting to schedule a pipeline where the sink requires batch index but source does not support it");
		}
	}
	idx_t max_threads = source_state->MaxThreads();
	return LaunchScanTasks(event, max_threads);
}

bool Pipeline::IsOrderDependent() const {
	auto &config = DBConfig::GetConfig(executor.context);
	if (!config.options.preserve_insertion_order) {
		return false;
	}
	if (this->sink_ && this->sink_->IsOrderDependent()) {
		return true;
	}
	if (this->source_ && this->source_->IsOrderDependent()) {
		return true;
	}
	for (auto &op : this->operators_) {
		if (op->IsOrderDependent()) {
			return true;
		}
	}
	return false;
}

void Pipeline::Schedule(shared_ptr<Event> &event) {
	D_ASSERT(ready);
	D_ASSERT(this->sink_);
	Reset();
	if (!ScheduleParallel(event)) {
		// could not parallelize this pipeline: push a sequential task instead
		ScheduleSequentialTask(event);
	}
}

bool Pipeline::LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads) {
	// split the scan up into parts and schedule the parts
	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	idx_t active_threads = scheduler.NumberOfThreads();
	if (max_threads > active_threads) {
		max_threads = active_threads;
	}
	if (max_threads <= 1) {
		// too small to parallelize
		return false;
	}

	// launch a task for every thread
	vector<unique_ptr<Task>> tasks;
	for (idx_t i = 0; i < max_threads; i++) {
		tasks.push_back(make_unique<PipelineTask>(*this, event));
	}
	event->SetTasks(move(tasks));
	return true;
}

void Pipeline::ResetSink() {
	if (this->sink_) {
		lock_guard<mutex> guard(this->sink_->lock);
		if (!this->sink_->sink_state) {
			this->sink_->sink_state = this->sink_->GetGlobalSinkState(GetClientContext());
		}
	}
}

void Pipeline::Reset() {
	ResetSink();
	for (auto &op : this->operators_) {
		if (op) {
			lock_guard<mutex> guard(op->lock);
			if (!op->op_state) {
				op->op_state = op->GetGlobalOperatorState(GetClientContext());
			}
		}
	}
	ResetSource(false);
	// we no longer reset source here because this function is no longer guaranteed to be called by the main thread
	// source reset needs to be called by the main thread because resetting a source may call into clients like R
	initialized = true;
}

void Pipeline::ResetSource(bool force) {
	if (force || !source_state) {
		source_state = this->source_->GetGlobalSourceState(GetClientContext());
	}
}

void Pipeline::Ready() {
	if (ready) {
		return;
	}
	ready = true;
	std::reverse(this->operators_.begin(), this->operators_.end());
}

void Pipeline::Finalize(Event &event) {
	if (executor.HasError()) {
		return;
	}
	D_ASSERT(ready);
	try {
		auto sink_state = this->sink_->Finalize(*this, event, executor.context, *this->sink_->sink_state);
		this->sink_->sink_state->state = sink_state;
	} catch (Exception &ex) { // LCOV_EXCL_START
		executor.PushError(PreservedError(ex));
	} catch (std::exception &ex) {
		executor.PushError(PreservedError(ex));
	} catch (...) {
		executor.PushError(PreservedError("Unknown exception in Finalize!"));
	} // LCOV_EXCL_STOP
}

void Pipeline::AddDependency(shared_ptr<Pipeline> &pipeline) {
	D_ASSERT(pipeline);
	this->dependencies_.push_back(weak_ptr<Pipeline>(pipeline));
	pipeline->parents.push_back(weak_ptr<Pipeline>(shared_from_this()));
}

string Pipeline::ToString() const {
	TreeRenderer renderer;
	return renderer.ToString(*this);
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

void Pipeline::PrintDependencies() const {
	for (auto &dep : this->dependencies_) {
		shared_ptr<Pipeline>(dep)->Print();
	}
}

vector<PhysicalOperator *> Pipeline::GetOperators() const {
	vector<PhysicalOperator *> result;
	D_ASSERT(this->source_);
	result.push_back(this->source_);
	result.insert(result.end(), this->operators_.begin(), this->operators_.end());
	if (this->sink_) {
		result.push_back(this->sink_);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Pipeline Build State
//===--------------------------------------------------------------------===//
void PipelineBuildState::SetPipelineSource(Pipeline &pipeline, PhysicalOperator *op) {
	pipeline.source_ = op;
}

void PipelineBuildState::SetPipelineSink(Pipeline &pipeline, PhysicalOperator *op, idx_t sink_pipeline_count) {
	pipeline.sink_ = op;
	// set the base batch index of this pipeline based on how many other pipelines have this node as their sink
	pipeline.base_batch_index = BATCH_INCREMENT * sink_pipeline_count;
}

void PipelineBuildState::AddPipelineOperator(Pipeline &pipeline, PhysicalOperator *op) {
	pipeline.operators_.push_back(op);
}

PhysicalOperator *PipelineBuildState::GetPipelineSource(Pipeline &pipeline) {
	return pipeline.source_;
}

PhysicalOperator *PipelineBuildState::GetPipelineSink(Pipeline &pipeline) {
	return pipeline.sink_;
}

void PipelineBuildState::SetPipelineOperators(Pipeline &pipeline, vector<PhysicalOperator *> operators) {
	pipeline.operators_ = move(operators);
}

shared_ptr<Pipeline> PipelineBuildState::CreateChildPipeline(Executor &executor, Pipeline &pipeline,
                                                             PhysicalOperator *op) {
	return executor.CreateChildPipeline(&pipeline, op);
}

vector<PhysicalOperator *> PipelineBuildState::GetPipelineOperators(Pipeline &pipeline) {
	return pipeline.operators_;
}

} // namespace duckdb
