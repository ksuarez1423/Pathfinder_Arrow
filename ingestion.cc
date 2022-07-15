#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/compute/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/dataset/api.h>

#include <iostream>

using arrow::Status;

namespace { 

Status read_csv(std::string path, arrow::Result<std::shared_ptr<arrow::Table> > &out){
  arrow::io::IOContext io_context = arrow::io::default_io_context();
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(path))

  arrow::Result<std::shared_ptr<arrow::csv::TableReader> > maybe_reader;
  ARROW_ASSIGN_OR_RAISE(maybe_reader, arrow::csv::TableReader::Make(io_context, infile, arrow::csv::ReadOptions::Defaults(),
    arrow::csv::ParseOptions::Defaults(), arrow::csv::ConvertOptions::Defaults()));
  std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;

  ARROW_ASSIGN_OR_RAISE(out, reader->Read());

  return Status::OK();
}

Status RunMain(int argc, char** argv) {

  arrow::Result<std::shared_ptr<arrow::Table> > basic_cr_ret; 
  ARROW_RETURN_NOT_OK(read_csv("data/Basic_CR.csv", basic_cr_ret));
  std::shared_ptr<arrow::Table> basic_cr = *basic_cr_ret;

  std::cout << basic_cr->ToString();

  arrow::Result<std::shared_ptr<arrow::Table> > hd_ret; 
  ARROW_RETURN_NOT_OK(read_csv("data/Type_HD.csv", hd_ret));
  std::shared_ptr<arrow::Table> hd_table = *hd_ret;

  std::cout << hd_table->ToString();

  arrow::Result<std::shared_ptr<arrow::Table> > type_stats_ret; 
  ARROW_RETURN_NOT_OK(read_csv("data/Type_Stats.csv", type_stats_ret));
  std::shared_ptr<arrow::Table> type_stats = *type_stats_ret;

  std::cout << type_stats->ToString();

  arrow::Result<std::shared_ptr<arrow::Table> > hd_stats_ret; 
  ARROW_RETURN_NOT_OK(read_csv("data/Other_Stats.csv", hd_stats_ret));
  std::shared_ptr<arrow::Table> hd_stats= *hd_stats_ret;
  
  std::cout << hd_stats->ToString();

  return Status::OK();
}
}
int main(int argc, char** argv) {
  Status st = RunMain(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
