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

Status calc_distance(std::shared_ptr<arrow::ChunkedArray> arr, int64_t in, int64_t &out){

  std::shared_ptr<arrow::Scalar> m_val;
  ARROW_ASSIGN_OR_RAISE(m_val, arrow::MakeScalar(arrow::int64(), in));

  arrow::compute::IndexOptions index_options;
  arrow::Datum datum; 
  ARROW_ASSIGN_OR_RAISE(datum, arrow::compute::Subtract(arr, m_val));
  arrow::Datum abs;
  ARROW_ASSIGN_OR_RAISE(abs, arrow::compute::AbsoluteValue({datum}));
  arrow::Datum min;
  ARROW_ASSIGN_OR_RAISE(min, arrow::compute::CallFunction("min", {abs}))
  arrow::Datum ind;
  index_options.value = min.scalar();
  ARROW_ASSIGN_OR_RAISE(ind, arrow::compute::CallFunction("index", {abs}, &index_options));
  out = ind.scalar_as<arrow::Int64Scalar>().value;

  return Status::OK();
}

Status RunMain(int argc, char** argv) {

  arrow::Result<std::shared_ptr<arrow::Table> > basic_cr_ret; 
  ARROW_RETURN_NOT_OK(read_csv("data/Basic_CR.csv", basic_cr_ret));
  std::shared_ptr<arrow::Table> basic_cr = *basic_cr_ret;

  arrow::Int64Builder builder; 
  std::shared_ptr<arrow::Array> CR_vals;

  //std::cout << basic_cr->ToString(); 
  //std::cout << basic_cr->GetColumnByName("Hit Points")->ToString();

  int64_t hp_cr;
  ARROW_RETURN_NOT_OK(calc_distance(basic_cr->GetColumnByName("Hit Points"), 44, hp_cr));
  std::cout << "CR from HP: " << hp_cr << std::endl;
  builder.Append(hp_cr);

  int64_t ac_cr;
  ARROW_RETURN_NOT_OK(calc_distance(basic_cr->GetColumnByName("Armor Class"), 18, ac_cr));
  std::cout << "CR from AC: " << ac_cr << std::endl;
  builder.Append(ac_cr);

  arrow::Datum two_col_add;
  ARROW_ASSIGN_OR_RAISE(two_col_add, arrow::compute::Add(basic_cr->GetColumnByName("Low Attack"), basic_cr->GetColumnByName("High Attack")));
  arrow::Datum two_col_avg;
  std::shared_ptr<arrow::Scalar> two;
  ARROW_ASSIGN_OR_RAISE(two, arrow::MakeScalar(arrow::int64(), 2));
  ARROW_ASSIGN_OR_RAISE(two_col_avg, arrow::compute::Divide(two_col_add, two));
  
  int64_t atk_cr;
  ARROW_RETURN_NOT_OK(calc_distance(two_col_avg.chunked_array(), 6, atk_cr));
  std::cout << "CR from attack bonus: " << atk_cr << std::endl;
  builder.Append(atk_cr);

  ARROW_ASSIGN_OR_RAISE(two_col_add, arrow::compute::Add(basic_cr->GetColumnByName("Damage High"), basic_cr->GetColumnByName("Damage Low")));
  ARROW_ASSIGN_OR_RAISE(two_col_avg, arrow::compute::Divide(two_col_add, two));
  
  int64_t dmg_cr;
  ARROW_RETURN_NOT_OK(calc_distance(two_col_avg.chunked_array(), 5, dmg_cr));
  std::cout << "CR from damage: " << dmg_cr << std::endl;
  builder.Append(dmg_cr);

  int64_t pdc_cr;
  ARROW_RETURN_NOT_OK(calc_distance(basic_cr->GetColumnByName("Primary Ability DC"), 17, pdc_cr));
  std::cout << "CR from ability DC: " << pdc_cr << std::endl;
  builder.Append(pdc_cr);

  int64_t gsave_cr;
  ARROW_RETURN_NOT_OK(calc_distance(basic_cr->GetColumnByName("Good Save"), 8, gsave_cr));
  std::cout << "CR from good save: " << gsave_cr << std::endl;
  builder.Append(gsave_cr);

  ARROW_RETURN_NOT_OK(calc_distance(basic_cr->GetColumnByName("Good Save"), 8, gsave_cr));
  std::cout << "CR from good save: " << gsave_cr << std::endl;
  builder.Append(gsave_cr);

  ARROW_RETURN_NOT_OK(calc_distance(basic_cr->GetColumnByName("Poor Save"), 4, gsave_cr));
  std::cout << "CR from poor save: " << gsave_cr << std::endl;
  builder.Append(gsave_cr);

  ARROW_ASSIGN_OR_RAISE(CR_vals, builder.Finish());

  arrow::Datum final_val;
  ARROW_ASSIGN_OR_RAISE(final_val, arrow::compute::Mean(CR_vals));
  ARROW_ASSIGN_OR_RAISE(final_val, arrow::compute::Round(final_val));
  std::cout << "Final CR: " << final_val.scalar()->ToString() << std::endl;

  //auto subtracted = datum.make_array();
  //std::cout << subtracted->ToString();

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
