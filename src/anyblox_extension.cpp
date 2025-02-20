#define DUCKDB_EXTENSION_MAIN

#include "anyblox_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <duckdb/common/types/arrow_string_view_type.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <fmt/core.h>
#include <anyblox/anyblox.hpp>
#include <shared_mutex>

namespace duckdb {
namespace {
const char *const CONFIG_WASM_VIRTUAL_MEMORY_LIMIT = "wasm_virtual_memory_limit";
const char *const CONFIG_WASM_CACHE_LIMIT = "wasm_cache_limit";
const char *const CONFIG_COMPILE_WITH_DEBUG = "compile_wasm_with_debug";
const char *const CONFIG_VALIDATE_UTF8 = "validate_utf8";
const int64_t DEFAULT_VIRTUAL_MEMORY_LIMIT = 16L * 1024 * 1024 * 1024;
const int64_t DEFAULT_WASM_CACHE_LIMIT = 256L * 1024 * 1024;

class AnyBloxRuntime {
public:
	[[nodiscard]] static anyblox::Runtime &Get();
	static anyblox::Runtime &GetOrInit(ClientContext &context_p);

private:
	static mutex runtime_mutex;
	static std::optional<anyblox::Runtime> ANYBLOX;
};

constinit mutex AnyBloxRuntime::runtime_mutex {};
constinit std::optional<anyblox::Runtime> AnyBloxRuntime::ANYBLOX {};

anyblox::Runtime &AnyBloxRuntime::Get() {
	return ANYBLOX.value();
}
anyblox::Runtime &AnyBloxRuntime::GetOrInit(ClientContext &context_p) {
	if (ANYBLOX.has_value()) {
		return ANYBLOX.value();
	} else {
		Value compile_with_debug {false}, virtual_memory_limit {DEFAULT_VIRTUAL_MEMORY_LIMIT},
		    wasm_cache_limit {DEFAULT_WASM_CACHE_LIMIT};
		context_p.TryGetCurrentSetting(CONFIG_COMPILE_WITH_DEBUG, compile_with_debug);
		context_p.TryGetCurrentSetting(CONFIG_WASM_VIRTUAL_MEMORY_LIMIT, virtual_memory_limit);
		context_p.TryGetCurrentSetting(CONFIG_WASM_CACHE_LIMIT, wasm_cache_limit);

		auto config_builder = anyblox::config::ConfigBuilder::create();
		config_builder.compile_with_debug(compile_with_debug.GetValue<bool>())
		    ->set_log_level(anyblox::config::LogLevel::Warn)
		    ->set_thread_virtual_memory_limit(virtual_memory_limit.GetValue<uint64_t>())
		    ->set_wasm_cache_limit(wasm_cache_limit.GetValue<uint64_t>());
		auto config = config_builder.build();
		{
			lock_guard<mutex> parallel_lock {runtime_mutex};
			if (ANYBLOX.has_value()) {
				return ANYBLOX.value();
			} else {
				ANYBLOX = anyblox::Runtime::create(std::move(config));
				return ANYBLOX.value();
			}
		}
	}
}

class AnyBloxFunctionData : public FunctionData {
public:
	explicit AnyBloxFunctionData(std::string anyblox_path, std::optional<std::string> data_path,
	                              anyblox::AnyBloxMetadata metadata)
	    : anyblox_path(std::move(anyblox_path)), data_path(std::move(data_path)), metadata(std::move(metadata)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<AnyBloxFunctionData>(*this);
	}

	bool Equals(const FunctionData &other) const override {
		const auto &other_data = other.Cast<AnyBloxFunctionData>();
		return anyblox_path == other_data.anyblox_path && data_path == other_data.data_path;
	}

	shared_ptr<anyblox::AnyBloxBundle> OpenBundle() const {
		std::shared_lock r_lock{cache_mutex};
		auto it = bundle_cache.find(anyblox_path);
		if (it == bundle_cache.end()) {
			r_lock.unlock();
			std::unique_lock w_lock{cache_mutex};
			it = bundle_cache.find(anyblox_path);
			if (it != bundle_cache.end()) {
				return it->second;
			}
			auto bundle = data_path.has_value()
				   ? anyblox::AnyBloxBundle::open_extension_and_data(anyblox_path, data_path.value())
				   : anyblox::AnyBloxBundle::open_self_contained(anyblox_path);
			auto ptr = make_shared_ptr<anyblox::AnyBloxBundle>(std::move(bundle));
			bundle_cache[anyblox_path] = ptr;
			return ptr;
		} else {
			return it->second;
		}
	}

	const anyblox::AnyBloxMetadata &GetMetadata() const {
		return metadata;
	}

	std::shared_ptr<arrow::Schema> GetSchema() const {
		return metadata.schema;
	}

private:
	std::string anyblox_path;
	std::optional<std::string> data_path;
	anyblox::AnyBloxMetadata metadata;

	static std::shared_mutex cache_mutex;
	static unordered_map<string, shared_ptr<anyblox::AnyBloxBundle>> bundle_cache;
};
std::shared_mutex AnyBloxFunctionData::cache_mutex {};
unordered_map<string, shared_ptr<anyblox::AnyBloxBundle>> AnyBloxFunctionData::bundle_cache {};

struct JobParams {
	uint64_t start_tuple;
	uint64_t tuple_count;
};

struct OutputColumnId {
	idx_t idx_in_array;
	idx_t idx_in_schema;
};

class AnyBloxGlobalState : public GlobalTableFunctionState {
public:
	AnyBloxGlobalState(shared_ptr<anyblox::AnyBloxBundle> bundle, std::shared_ptr<arrow::Schema> schema,
	                    vector<column_t> column_ids, uint64_t max_threads)
	    : bundle(std::move(bundle)), schema(std::move(schema)) {
		row_count = this->bundle->metadata().data.count;
		rows_per_job = std::max(row_count / max_threads, MIN_JOB_SIZE);
		uint64_t actual_max_threads = (row_count + rows_per_job - 1) / rows_per_job;
		this->max_threads = actual_max_threads;
		this->next_job_start = 0;

		projection = anyblox::ColumnProjection::from_indices(column_ids.begin(), column_ids.end());
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

	const anyblox::AnyBloxBundle &GetBundle() const {
		return *bundle;
	}

	const anyblox::ColumnProjection &GetColumnProjection() const {
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

	shared_ptr<anyblox::AnyBloxBundle> bundle;
	std::shared_ptr<arrow::Schema> schema;
	anyblox::ColumnProjection projection;
	vector<OutputColumnId> output_column_ids;

	uint64_t max_threads;
	uint64_t next_job_start;
	uint64_t row_count;
	uint64_t rows_per_job;
};

class AnyBloxLocalState : public LocalTableFunctionState {
public:
	explicit AnyBloxLocalState(const ClientContext &context, const anyblox::AnyBloxBundle &bundle,
	                            anyblox::ColumnProjection column_projection);

	void ReadBatch(DataChunk &output, const vector<OutputColumnId> &output_column_ids);
	bool IsFinished() const;
	void SetNewJob(JobParams job_params);

private:
	std::unique_ptr<anyblox::ThreadLocalDecodeJob> anyblox_job;
	shared_ptr<ArrowArrayWrapper> current_batch;
	idx_t current_batch_idx = 0;
	JobParams job_params;
	uint64_t batch_size;

	static std::unique_ptr<anyblox::ThreadLocalDecodeJob>
	InitAnyBloxJob(const ClientContext &context, const anyblox::AnyBloxBundle &bundle,
	                anyblox::ColumnProjection column_projection);

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

AnyBloxLocalState::AnyBloxLocalState(const ClientContext &context, const anyblox::AnyBloxBundle &bundle,
                                       anyblox::ColumnProjection column_projection)
    : anyblox_job(InitAnyBloxJob(context, bundle, column_projection)), current_batch(nullptr),
      job_params(JobParams {.start_tuple = 0, .tuple_count = 0}) {
	batch_size = static_cast<uint64_t>(STANDARD_VECTOR_SIZE) * 16;
	auto min_batch_size = bundle.metadata().decoder.min_batch_size;
	if (min_batch_size) {
		batch_size = std::max(min_batch_size.value(), batch_size);
	}
}

std::unique_ptr<anyblox::ThreadLocalDecodeJob>
AnyBloxLocalState::InitAnyBloxJob(const ClientContext &context, const anyblox::AnyBloxBundle &bundle,
                                    anyblox::ColumnProjection column_projection) {
	Value validate_utf8 {true};
	context.TryGetCurrentSetting(CONFIG_VALIDATE_UTF8, validate_utf8);
	auto builder = anyblox::JobParameterBuilder {};
	if (!validate_utf8.GetValue<bool>()) {
		builder.do_not_validate_utf8();
	}
	anyblox::JobParameters params = builder.with_column_projection(column_projection).finish(bundle);
	return AnyBloxRuntime::Get().decode_job_init(params).ValueOrDie();
}

void AnyBloxLocalState::ReadBatch(DataChunk &output, const vector<OutputColumnId> &output_column_ids) {
	if (RemainingInBatch() == 0) {
		auto request_size = std::min(batch_size, job_params.tuple_count);
		current_batch = make_shared_ptr<ArrowArrayWrapper>();
		current_batch->arrow_array =
		    AnyBloxRuntime::Get().decode_batch(anyblox_job, job_params.start_tuple, request_size);
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
		auto arrow_type = anyblox_job->schema()->fields()[idx_in_schema]->type();
		D_ASSERT(column_array.release);
		D_ASSERT(column_array.length == array.length);

		SetValidityMask(output.data[idx], column_array, current_batch_idx, size_to_write);
		ColumnArrowToDuckDB(output.data[idx], column_array, *arrow_type, current_batch_idx, size_to_write);
		output.data[idx].GetBuffer()->SetAuxiliaryData(make_uniq<ArrayHandleAuxiliaryData>(current_batch));
	}

	current_batch_idx += size_to_write;
	output.SetCardinality(size_to_write);
}

bool AnyBloxLocalState::IsFinished() const {
	return RemainingInBatch() == 0 && job_params.tuple_count == 0;
}

void AnyBloxLocalState::SetNewJob(JobParams job_params) {
	this->job_params = job_params;
}

void AnyBloxLocalState::SetValidityMask(Vector &vector, ArrowArray &array, idx_t offset, size_t len) {
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

void AnyBloxLocalState::ColumnArrowToDuckDB(Vector &vector, ArrowArray &array, const arrow::DataType &arrow_type,
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

void AnyBloxLocalState::SetVectorString(Vector &vector, const char *cdata, const uint32_t *offsets, idx_t size) {
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

void AnyBloxLocalState::SetVectorStringView(Vector &vector, idx_t size, ArrowArray &array, idx_t offset) {
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

void AnyBloxFunction(ClientContext & /* context */, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state.get()->Cast<AnyBloxGlobalState>();
	auto &local_state = data.local_state.get()->Cast<AnyBloxLocalState>();

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

unique_ptr<FunctionData> AnyBloxBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &logicals, vector<string> &names) {
	AnyBloxRuntime::GetOrInit(context);
	assert(input.inputs.size() == 1);
	auto anyblox_path = input.inputs.front().GetValue<std::string>();
	auto data_path_iter = input.named_parameters.find("data");
	std::optional<std::string> data_path = data_path_iter != input.named_parameters.end()
	                                           ? data_path_iter->second.GetValue<std::string>()
	                                           : std::optional<std::string> {};
	anyblox::AnyBloxBundle bundle =
	    data_path.has_value() ? anyblox::AnyBloxBundle::open_extension_and_data(anyblox_path, data_path.value())
	                          : anyblox::AnyBloxBundle::open_self_contained(anyblox_path);
	anyblox::AnyBloxMetadata metadata = bundle.metadata();

	for (const auto &field : metadata.schema->fields()) {
		names.emplace_back(field->name());
		LogicalType type = TranslateArrowType(*field->type());
		logicals.emplace_back(type);
	}

	return make_uniq<AnyBloxFunctionData>(anyblox_path, data_path, std::move(metadata));
}

unique_ptr<GlobalTableFunctionState> AnyBloxGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	const auto &data = input.bind_data.get()->Cast<AnyBloxFunctionData>();
	auto bundle = data.OpenBundle();
	uint64_t max_threads = context.db->NumberOfThreads();

	return make_uniq<AnyBloxGlobalState>(std::move(bundle), std::move(data.GetSchema()), input.column_ids,
	                                      max_threads);
}

unique_ptr<LocalTableFunctionState> AnyBloxLocalInit(ExecutionContext &context, TableFunctionInitInput & /* input */,
                                                      GlobalTableFunctionState *global_state) {
	auto &anyblox_state = global_state->Cast<AnyBloxGlobalState>();
	return make_uniq<AnyBloxLocalState>(context.client, anyblox_state.GetBundle(),
	                                     anyblox_state.GetColumnProjection());
}

unique_ptr<NodeStatistics> AnyBloxCardinality(ClientContext & /* context */, const FunctionData *bind_data) {
	const auto &anyblox_data = bind_data->Cast<AnyBloxFunctionData>();
	auto row_count = anyblox_data.GetMetadata().data.count;
	return make_uniq<NodeStatistics>(row_count, row_count);
}

unique_ptr<BaseStatistics> AnyBloxStatistics(ClientContext & /* context */, const FunctionData *bind_data,
                                              column_t column_index) {
	if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	const auto &anyblox_data = bind_data->Cast<AnyBloxFunctionData>();
	const auto &column = anyblox_data.GetMetadata().schema->fields()[column_index];

	auto stats = BaseStatistics::CreateUnknown(TranslateArrowType(*column->type()));
	if (!column->nullable()) {
		stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
	}

	return make_uniq<BaseStatistics>(std::move(stats));
}

static void LoadInternal(DatabaseInstance &instance) {
	auto anyblox_table_function = TableFunction("anyblox", {LogicalTypeId::VARCHAR}, AnyBloxFunction, AnyBloxBind,
	                                             AnyBloxGlobalInit, AnyBloxLocalInit);
	anyblox_table_function.projection_pushdown = true;
	anyblox_table_function.filter_pushdown = false;
	anyblox_table_function.filter_prune = false;
	anyblox_table_function.named_parameters.insert({"data", LogicalTypeId::VARCHAR});
	anyblox_table_function.cardinality = AnyBloxCardinality;
	anyblox_table_function.statistics = AnyBloxStatistics;
	ExtensionUtil::RegisterFunction(instance, anyblox_table_function);

	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption(CONFIG_WASM_VIRTUAL_MEMORY_LIMIT,
	                          "Upper limit for all virtual allocations performed by the wasm runtime",
	                          LogicalTypeId::BIGINT, Value(DEFAULT_VIRTUAL_MEMORY_LIMIT));
	config.AddExtensionOption(CONFIG_WASM_CACHE_LIMIT, "Upper limit for the wasm compiled decoder cache",
	                          LogicalTypeId::BIGINT, Value(DEFAULT_WASM_CACHE_LIMIT));
	config.AddExtensionOption(CONFIG_COMPILE_WITH_DEBUG, "Whether to compile wasm decoders with debuginfo",
	                          LogicalTypeId::BOOLEAN, Value(false));
	config.AddExtensionOption(
	    CONFIG_VALIDATE_UTF8,
	    "Whether to check string columns returned from decoders are valid UTF8. WARNING: disabling this is unsafe, "
	    "untrusted wasm decoders can cause memory security vulnerabilities with invalid strings.",
	    LogicalTypeId::BOOLEAN, Value(true));
}

void AnybloxExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string AnybloxExtension::Name() {
	return "anyblox";
}

std::string AnybloxExtension::Version() const {
#ifdef EXT_VERSION_ANYBLOX
	return EXT_VERSION_ANYBLOX;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void anyblox_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::AnybloxExtension>();
}

DUCKDB_EXTENSION_API const char *anyblox_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
