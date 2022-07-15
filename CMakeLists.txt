project(PFArrow)
set (CMAKE_CXX_STANDARD 11)
find_package(Arrow REQUIRED) 
get_filename_component(ARROW_CONFIG_PATH ${Arrow_CONFIG} DIRECTORY)
find_package(Parquet REQUIRED HINTS ${ARROW_CONFIG_PATH})
find_package(ArrowDataset REQUIRED HINTS ${ARROW_CONFIG_PATH})

add_executable(ingestion ingestion.cc)
target_link_libraries(ingestion PRIVATE arrow_shared arrow_dataset_shared)
