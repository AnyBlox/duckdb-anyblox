#define DUCKDB_EXTENSION_MAIN

#include "ignition_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <duckdb/common/types/arrow_string_view_type.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <fmt/core.h>
#include <ignition/ignition.hpp>

namespace duckdb {
namespace {
ignition::Runtime InitRuntime() {
	auto config_builder = ignition::config::ConfigBuilder::create();
	config_builder.compile_with_debug(true)
	    ->set_log_level(ignition::config::LogLevel::Debug)
	    ->set_memory_cache_virtual_memory_limit(64UL * 1024 * 1024 * 1024 * 1024)
	    ->set_wasm_cache_limit(64UL * 1024 * 1024);
	auto config = config_builder.build();
	return ignition::Runtime::create(std::move(config));
}
ignition::Runtime IGNITION = InitRuntime();

class IgnitionFunctionData : public FunctionData {
public:
	explicit IgnitionFunctionData(std::string ignition_path, std::optional<std::string> data_path,
	                              ignition::IgnitionMetadata metadata)
	    : ignition_path(std::move(ignition_path)), data_path(std::move(data_path)), metadata(std::move(metadata)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IgnitionFunctionData>(*this);
	}

	bool Equals(const FunctionData &other) const override {
		const auto &other_data = other.Cast<IgnitionFunctionData>();
		return ignition_path == other_data.ignition_path && data_path == other_data.data_path;
	}

	ignition::IgnitionBundle OpenBundle() const {
		return data_path.has_value()
		           ? ignition::IgnitionBundle::open_extension_and_data(ignition_path, data_path.value())
		           : ignition::IgnitionBundle::open_self_contained(ignition_path);
	}

	const ignition::IgnitionMetadata &GetMetadata() const {
		return metadata;
	}

	std::shared_ptr<arrow::Schema> GetSchema() const {
		return metadata.schema;
	}

private:
	std::string ignition_path;
	std::optional<std::string> data_path;
	ignition::IgnitionMetadata metadata;
};

struct JobParams {
	uint64_t start_tuple;
	uint64_t tuple_count;
};

struct OutputColumnId {
	idx_t idx_in_array;
	idx_t idx_in_schema;
};

class IgnitionGlobalState : public GlobalTableFunctionState {
public:
	IgnitionGlobalState(ignition::IgnitionBundle bundle, std::shared_ptr<arrow::Schema> schema,
	                    vector<column_t> column_ids, uint64_t max_threads)
	    : bundle(std::move(bundle)), schema(std::move(schema)) {
		row_count = this->bundle.metadata().data.count;
		rows_per_job = std::max(row_count / max_threads, MIN_JOB_SIZE);
		uint64_t actual_max_threads = (row_count + rows_per_job - 1) / rows_per_job;
		this->max_threads = actual_max_threads;
		this->next_job_start = 0;

		projection = ignition::ColumnProjection::from_indices(column_ids.begin(), column_ids.end());
		vector<std::pair<column_t, idx_t>> column_projection {};
		column_projection.reserve(column_ids.size());
		for (idx_t i = 0, limit = column_ids.size(); i < limit; i += 1) {
			column_projection.emplace_back(column_ids[i], i);
		}
		std::ranges::sort(column_projection.begin(), column_projection.end());
		output_column_ids.resize(column_ids.size());
		for (idx_t i = 0; i < column_projection.size(); i += 1) {
			auto [col, id] = column_projection[i];
			output_column_ids[id] = OutputColumnId {.idx_in_array = i, .idx_in_schema = col};
		}
	}

	const ignition::IgnitionBundle &GetBundle() const {
		return bundle;
	}

	const ignition::ColumnProjection &GetColumnProjection() const {
		return projection;
	}

	const vector<OutputColumnId> &GetOutputColumnIds() const {
		return output_column_ids;
	}

	std::optional<JobParams> AcquireNextJob() {
		uint64_t start_tuple;

		{
			lock_guard<mutex> parallel_lock {main_mutex};
			if (next_job_start >= row_count) {
				return {};
			}
			start_tuple = next_job_start;
			next_job_start += rows_per_job;
		}

		JobParams job = {.start_tuple = start_tuple, .tuple_count = std::min(rows_per_job, row_count - start_tuple)};

		return job;
	}

	idx_t MaxThreads() const override {
		return max_threads;
	}

private:
	const uint64_t MIN_JOB_SIZE = STANDARD_VECTOR_SIZE;

	mutable mutex main_mutex;

	ignition::IgnitionBundle bundle;
	std::shared_ptr<arrow::Schema> schema;
	ignition::ColumnProjection projection;
	vector<OutputColumnId> output_column_ids;

	uint64_t max_threads;
	uint64_t next_job_start;
	uint64_t row_count;
	uint64_t rows_per_job;
};

class IgnitionLocalState : public LocalTableFunctionState {
public:
	explicit IgnitionLocalState(const ignition::IgnitionBundle &bundle, ignition::ColumnProjection column_projection)
	    : ignition_job(std::move(IGNITION.decode_job_init(bundle, column_projection)).ValueOrDie()),
	      current_batch(nullptr), job_params(JobParams {.start_tuple = 0, .tuple_count = 0}) {
	}

	void ReadBatch(DataChunk &output, const vector<OutputColumnId> &output_column_ids);
	bool IsFinished() const;
	void SetNewJob(JobParams job_params);

private:
	std::unique_ptr<ignition::ThreadLocalDecodeJob> ignition_job;
	shared_ptr<ArrowArrayWrapper> current_batch;
	idx_t current_batch_idx = 0;
	JobParams job_params;

	uint64_t RemainingInBatch() const {
		return current_batch == nullptr ? 0 : current_batch->arrow_array.length - current_batch_idx;
	}

	static void SetValidityMask(Vector &vector, ArrowArray &array, idx_t offset, size_t len);
	static void ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, const arrow::DataType &arrow_type, idx_t offset,
	                                size_t len);
	static void SetVectorString(Vector &vector, const char *cdata, const uint32_t *offsets, idx_t size);
	static void SetVectorStringView(Vector &vector, idx_t size, ArrowArray &array, idx_t offset);

	class ArrayHandleAuxiliaryData : public VectorAuxiliaryData {
	public:
		explicit ArrayHandleAuxiliaryData(shared_ptr<ArrowArrayWrapper> array)
		    : VectorAuxiliaryData(VectorAuxiliaryDataType::ARROW_AUXILIARY), array(std::move(array)) {
		}

	private:
		shared_ptr<ArrowArrayWrapper> array;
	};
};

void IgnitionLocalState::ReadBatch(DataChunk &output, const vector<OutputColumnId> &output_column_ids) {
	if (RemainingInBatch() == 0) {
		auto request_size = std::min(static_cast<uint64_t>(STANDARD_VECTOR_SIZE) * 50, job_params.tuple_count);
		current_batch = make_shared_ptr<ArrowArrayWrapper>();
		current_batch->arrow_array = IGNITION.decode_batch(ignition_job, job_params.start_tuple, request_size);
		current_batch_idx = 0;
		job_params.start_tuple += current_batch->arrow_array.length;
		job_params.tuple_count -= current_batch->arrow_array.length;
	}

	auto &array = current_batch->arrow_array;
	auto size_to_write = std::min(static_cast<uint64_t>(STANDARD_VECTOR_SIZE), RemainingInBatch());

	D_ASSERT(array.n_children == (int64_t)output.ColumnCount());
	D_ASSERT(array.release);
	for (idx_t idx = 0; idx < output.ColumnCount(); idx += 1) {
		auto [idx_in_array, idx_in_schema] = output_column_ids[idx];
		auto &column_array = *array.children[idx_in_array];
		auto arrow_type = ignition_job->schema()->fields()[idx_in_schema]->type();
		D_ASSERT(column_array.release);
		D_ASSERT(column_array.length == array.length);

		SetValidityMask(output.data[idx], column_array, current_batch_idx, size_to_write);
		ColumnArrowToDuckDB(output.data[idx], column_array, *arrow_type, current_batch_idx, size_to_write);
		output.data[idx].GetBuffer()->SetAuxiliaryData(make_uniq<ArrayHandleAuxiliaryData>(current_batch));
	}

	current_batch_idx += size_to_write;
	output.SetCardinality(size_to_write);
}

bool IgnitionLocalState::IsFinished() const {
	return RemainingInBatch() == 0 && job_params.tuple_count == 0;
}

void IgnitionLocalState::SetNewJob(JobParams job_params) {
	this->job_params = job_params;
}

void IgnitionLocalState::SetValidityMask(Vector &vector, ArrowArray &array, idx_t offset, size_t len) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(offset % 8 == 0);
	auto &mask = FlatVector::Validity(vector);

	if (array.null_count == 0 || array.n_buffers == 0 || array.buffers[0] == nullptr) {
		return;
	}

	mask.EnsureWritable();
	auto n_bitmask_bytes = (len + 8 - 1) / 8;
	const void *src_ptr = static_cast<const uint8_t *>(array.buffers[0]) + offset;
	memcpy(mask.GetData(), src_ptr, n_bitmask_bytes);
}

void IgnitionLocalState::ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, const arrow::DataType &arrow_type,
                                             idx_t offset, size_t len) {
	switch (vector.GetType().id()) {
	case LogicalTypeId::SQLNULL:
		vector.Reference(Value {});
		break;
	case LogicalTypeId::BOOLEAN: {
		//! Arrow bit-packs boolean values
		//! Lets first figure out where we are in the source array
		D_ASSERT(offset % 8 == 0);
		auto src_ptr = static_cast<const uint8_t *>(array.buffers[1]) + offset / 8;
		auto tgt_ptr = FlatVector::GetData(vector);
		int src_pos = 0;
		idx_t cur_bit = 0;
		for (idx_t row = 0; row < len; row++) {
			if ((src_ptr[src_pos] & (1 << cur_bit)) == 0) {
				tgt_ptr[row] = 0;
			} else {
				tgt_ptr[row] = 1;
			}
			cur_bit++;
			if (cur_bit == 8) {
				src_pos++;
				cur_bit = 0;
			}
		}
		break;
	}
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIME_TZ: {
		auto ptr_offset = offset * arrow_type.byte_width();
		auto data_ptr = const_cast<uint8_t *>(static_cast<const uint8_t *>(array.buffers[1])) + ptr_offset; // NOLINT
		FlatVector::SetData(vector, data_ptr);
		break;
	}
	case LogicalTypeId::DATE: {
		const auto &arrow_date_type = static_cast<const arrow::DateType &>(arrow_type);
		switch (arrow_date_type.unit()) {
		case arrow::DateUnit::DAY: {
			auto ptr_offset = offset * arrow_type.byte_width();
			auto data_ptr =
			    const_cast<uint8_t *>(static_cast<const uint8_t *>(array.buffers[1])) + ptr_offset; // NOLINT
			FlatVector::SetData(vector, data_ptr);
			break;
		}
		case arrow::DateUnit::MILLI:
		default:
			throw NotImplementedException("Unsupported precision for Date Type ");
		}
		break;
	}
	case LogicalTypeId::VARCHAR: {
		switch (arrow_type.id()) {
		case arrow::Type::STRING: {
			auto data = static_cast<const char *>(array.buffers[2]);
			auto offsets = static_cast<const uint32_t *>(array.buffers[1]) + offset;
			SetVectorString(vector, data, offsets, len);
			break;
		}
		case arrow::Type::STRING_VIEW: {
			SetVectorStringView(vector, len, array, offset);
			break;
		}
		case arrow::Type::FIXED_SIZE_BINARY: {
			assert(arrow_type.byte_width() == 1);
			auto data = static_cast<const char *>(array.buffers[1]) + offset;
			auto strings = FlatVector::GetData<string_t>(vector);
			for (idx_t row_idx = 0; row_idx < len; row_idx++) {
				if (FlatVector::IsNull(vector, row_idx)) {
					continue;
				}
				strings[row_idx] = string_t(data + row_idx, 1);
			}
			break;
		}
		default:
			throw NotImplementedException("Unsupported Arrow string type in translation. " + arrow_type.ToString());
		}
		break;
	}
	default:
		throw NotImplementedException("Unsupported LogicalType in translation. " + arrow_type.ToString());
	}
}

void IgnitionLocalState::SetVectorString(Vector &vector, const char *cdata, const uint32_t *offsets, idx_t size) {
	auto strings = FlatVector::GetData<string_t>(vector);
	for (idx_t row_idx = 0; row_idx < size; row_idx++) {
		if (FlatVector::IsNull(vector, row_idx)) {
			continue;
		}
		auto cptr = cdata + offsets[row_idx];
		auto str_len = offsets[row_idx + 1] - offsets[row_idx];
		strings[row_idx] = string_t(cptr, str_len);
	}
}

void IgnitionLocalState::SetVectorStringView(Vector &vector, idx_t size, ArrowArray &array, idx_t offset) {
	auto strings = FlatVector::GetData<string_t>(vector);
	auto arrow_string = static_cast<const arrow_string_view_t *>(array.buffers[1]) + offset;

	for (idx_t row_idx = 0; row_idx < size; row_idx++) {
		if (FlatVector::IsNull(vector, row_idx)) {
			continue;
		}
		auto length = UnsafeNumericCast<uint32_t>(arrow_string[row_idx].Length());
		if (arrow_string[row_idx].IsInline()) {
			//	This string is inlined
			//  | Bytes 0-3  | Bytes 4-15                            |
			//  |------------|---------------------------------------|
			//  | length     | data (padded with 0)                  |
			strings[row_idx] = string_t(arrow_string[row_idx].GetInlineData(), length);
		} else {
			//  This string is not inlined, we have to check a different buffer and offsets
			//  | Bytes 0-3  | Bytes 4-7  | Bytes 8-11 | Bytes 12-15 |
			//  |------------|------------|------------|-------------|
			//  | length     | prefix     | buf. index | offset      |
			auto buffer_index = UnsafeNumericCast<uint32_t>(arrow_string[row_idx].GetBufferIndex());
			int32_t str_offset = arrow_string[row_idx].GetOffset();
			D_ASSERT(array.n_buffers > 2 + buffer_index);
			auto c_data = static_cast<const char *>(array.buffers[2 + buffer_index]);
			strings[row_idx] = string_t(&c_data[str_offset], length);
		}
	}
}

LogicalType TranslateArrowType(const arrow::DataType &data_type) {
	switch (data_type.id()) {

	case arrow::Type::NA:
		return LogicalType::SQLNULL;
	case arrow::Type::BOOL:
		return LogicalType::BOOLEAN;
	case arrow::Type::INT8:
		return LogicalType::TINYINT;
	case arrow::Type::UINT8:
		return LogicalType::UTINYINT;
	case arrow::Type::INT16:
		return LogicalType::SMALLINT;
	case arrow::Type::UINT16:
		return LogicalType::USMALLINT;
	case arrow::Type::INT32:
		return LogicalType::INTEGER;
	case arrow::Type::UINT32:
		return LogicalType::UINTEGER;
	case arrow::Type::INT64:
		return LogicalType::BIGINT;
	case arrow::Type::UINT64:
		return LogicalType::UBIGINT;
	case arrow::Type::HALF_FLOAT:
		throw NotImplementedException("Unsupported Arrow Type HALF_FLOAT");
	case arrow::Type::FLOAT:
		return LogicalType::FLOAT;
	case arrow::Type::DOUBLE:
		return LogicalType::DOUBLE;
	case arrow::Type::STRING:
	case arrow::Type::LARGE_STRING:
	case arrow::Type::STRING_VIEW:
		return LogicalType::VARCHAR;
	case arrow::Type::BINARY:
	case arrow::Type::LARGE_BINARY:
	case arrow::Type::BINARY_VIEW:
		return LogicalType::BLOB;
	case arrow::Type::FIXED_SIZE_BINARY:
		if (data_type.byte_width() == 1) {
			return LogicalType::VARCHAR;
		} else {
			return LogicalType::BLOB;
		}
	case arrow::Type::DATE32:
	case arrow::Type::DATE64:
		return LogicalType::DATE;
	case arrow::Type::TIMESTAMP:
		break;
	case arrow::Type::TIME32:
		break;
	case arrow::Type::TIME64:
		break;
	case arrow::Type::INTERVAL_MONTHS:
		break;
	case arrow::Type::INTERVAL_DAY_TIME:
		break;
	case arrow::Type::DECIMAL128:
		break;
	case arrow::Type::DECIMAL256:
		break;
	case arrow::Type::LIST:
		break;
	case arrow::Type::STRUCT:
		break;
	case arrow::Type::SPARSE_UNION:
		break;
	case arrow::Type::DENSE_UNION:
		break;
	case arrow::Type::DICTIONARY:
		break;
	case arrow::Type::MAP:
		break;
	case arrow::Type::EXTENSION:
		break;
	case arrow::Type::FIXED_SIZE_LIST:
		break;
	case arrow::Type::DURATION:
		break;
	case arrow::Type::LARGE_LIST:
		break;
	case arrow::Type::INTERVAL_MONTH_DAY_NANO:
		break;
	case arrow::Type::RUN_END_ENCODED:
		break;
	case arrow::Type::LIST_VIEW:
		break;
	case arrow::Type::LARGE_LIST_VIEW:
		break;
	case arrow::Type::DECIMAL32:
		break;
	case arrow::Type::DECIMAL64:
		break;
	case arrow::Type::MAX_ID:
		break;
	}
	throw NotImplementedException("Unsupported Arrow Type" + data_type.ToString());
}
} // namespace

void IgnitionFunction(ClientContext & /* context */, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state.get()->Cast<IgnitionGlobalState>();
	auto &local_state = data.local_state.get()->Cast<IgnitionLocalState>();

	if (local_state.IsFinished()) {
		auto new_job = global_state.AcquireNextJob();
		if (!new_job) {
			output.SetCardinality(0);
			return;
		}

		local_state.SetNewJob(new_job.value());
	}

	local_state.ReadBatch(output, global_state.GetOutputColumnIds());
}

unique_ptr<FunctionData> IgnitionBind(ClientContext & /* context */, TableFunctionBindInput &input,
                                      vector<LogicalType> &logicals, vector<string> &names) {
	assert(input.inputs.size() == 1);
	auto ignition_path = input.inputs.front().GetValue<std::string>();
	auto data_path_iter = input.named_parameters.find("data");
	std::optional<std::string> data_path = data_path_iter != input.named_parameters.end()
	                                           ? data_path_iter->second.GetValue<std::string>()
	                                           : std::optional<std::string> {};
	ignition::IgnitionBundle bundle =
	    data_path.has_value() ? ignition::IgnitionBundle::open_extension_and_data(ignition_path, data_path.value())
	                          : ignition::IgnitionBundle::open_self_contained(ignition_path);
	ignition::IgnitionMetadata metadata = bundle.metadata();

	for (const auto &field : metadata.schema->fields()) {
		names.emplace_back(field->name());
		LogicalType type = TranslateArrowType(*field->type());
		logicals.emplace_back(type);
	}

	return make_uniq<IgnitionFunctionData>(ignition_path, data_path, std::move(metadata));
}

unique_ptr<GlobalTableFunctionState> IgnitionGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	const auto &data = input.bind_data.get()->Cast<IgnitionFunctionData>();
	auto bundle = data.OpenBundle();
	uint64_t max_threads = context.db->NumberOfThreads();

	return make_uniq<IgnitionGlobalState>(std::move(bundle), std::move(data.GetSchema()), input.column_ids,
	                                      max_threads);
}

unique_ptr<LocalTableFunctionState> IgnitionLocalInit(ExecutionContext & /* context */,
                                                      TableFunctionInitInput & /* input */,
                                                      GlobalTableFunctionState *global_state) {
	auto &ignition_state = global_state->Cast<IgnitionGlobalState>();
	return make_uniq<IgnitionLocalState>(ignition_state.GetBundle(), ignition_state.GetColumnProjection());
}

unique_ptr<NodeStatistics> IgnitionCardinality(ClientContext & /* context */, const FunctionData *bind_data) {
	const auto &ignition_data = bind_data->Cast<IgnitionFunctionData>();
	auto row_count = ignition_data.GetMetadata().data.count;
	return make_uniq<NodeStatistics>(row_count, row_count);
}

unique_ptr<BaseStatistics> IgnitionStatistics(ClientContext & /* context */, const FunctionData *bind_data,
                                              column_t column_index) {
	if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	const auto &ignition_data = bind_data->Cast<IgnitionFunctionData>();
	const auto &column = ignition_data.GetMetadata().schema->fields()[column_index];

	auto stats = BaseStatistics::CreateUnknown(TranslateArrowType(*column->type()));
	if (!column->nullable()) {
		stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
	}

	return make_uniq<BaseStatistics>(std::move(stats));
}

static void LoadInternal(DatabaseInstance &instance) {
	auto ignition_table_function = TableFunction("ignition", {LogicalTypeId::VARCHAR}, IgnitionFunction, IgnitionBind,
	                                             IgnitionGlobalInit, IgnitionLocalInit);
	ignition_table_function.projection_pushdown = true;
	ignition_table_function.filter_pushdown = false;
	ignition_table_function.filter_prune = false;
	ignition_table_function.named_parameters.insert({"data", LogicalTypeId::VARCHAR});
	ignition_table_function.cardinality = IgnitionCardinality;
	ignition_table_function.statistics = IgnitionStatistics;
	ExtensionUtil::RegisterFunction(instance, ignition_table_function);
}

void IgnitionExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string IgnitionExtension::Name() {
	return "ignition";
}

std::string IgnitionExtension::Version() const {
#ifdef EXT_VERSION_IGNITION
	return EXT_VERSION_IGNITION;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void ignition_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::IgnitionExtension>();
}

DUCKDB_EXTENSION_API const char *ignition_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
