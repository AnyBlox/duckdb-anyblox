cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ignition)

set(CMAKE_CXX_STANDARD 20)

# Ignition
find_package(IgnitionCpp 0.9.0 REQUIRED)
message(STATUS "Found Ignition CPP library '${IgnitionCpp_VERSION}'")


set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/ignition_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} Ignition::IgnitionCpp)
target_link_libraries(${LOADABLE_EXTENSION_NAME} Ignition::IgnitionCpp)

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
