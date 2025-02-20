#pragma once

#include "duckdb.hpp"

namespace duckdb {
// The name has to be Anyblox (not AnyBlox) for DuckDB to find the extension.
class AnybloxExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};
} // namespace duckdb
