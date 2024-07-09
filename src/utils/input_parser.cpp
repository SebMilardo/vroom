/*

This file is part of VROOM.

Copyright (c) 2015-2024, Julien Coupey.
All rights reserved (see LICENSE).

*/

#include <algorithm>

#include "../include/simdjson/singleheader/simdjson.h"

#include "utils/input_parser.h"

namespace vroom::io {


void parse(Input& input, const std::string& input_str, bool geometry) {
  // Parse and raise parsing errors
  simdjson::padded_string padded_input_str(input_str);
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc = parser.iterate(padded_input_str);

  simdjson::ondemand::object obj;
  auto error = doc.get_object().get(obj);
  if (error) { throw InputException("Error while parsing."); }
  // Iterate over the document
  for (auto field : obj) {
  
    simdjson::ondemand::raw_json_string key;
    auto error = field.key().get(key);
    if (error) { throw InputException("Error while parsing."); }
    if (key == "jobs") {
      std::cout << "JOBS:" << std::endl;
      simdjson::ondemand::array jobs;
      error = field.value().get_array().get(jobs);
      if (error) { throw InputException("Error while parsing jobs."); }
      for (simdjson::ondemand::object job : jobs){
        std::cout << job.raw_json() << std::endl;
      }
    } else if (key == "shipments") {
      std::cout << "SHIPMENTS:" << std::endl;
      simdjson::ondemand::array shipments;
      error = field.value().get_array().get(shipments);
      if (error) { throw InputException("Error while parsing shipments."); }
      for (simdjson::ondemand::object shipment : shipments){
        std::cout << shipment.raw_json() << std::endl;
      }
    } else if (key == "vehicles") {
      std::cout << "VEHICLES:" << std::endl;
      simdjson::ondemand::array vehicles;
      error = field.value().get_array().get(vehicles);
      if (error) { throw InputException("Error while parsing vehicles."); }
      for (simdjson::ondemand::object vehicle : vehicles){
        std::cout << vehicle.raw_json() << std::endl;
      }
    } else if (key == "matrices") {
      std::cout << "MATRICES:" << std::endl;
      simdjson::ondemand::object matrices;
      error = field.value().get_object().get(matrices);
      if (error) { throw InputException("Error while parsing matrices."); }
      for (auto matrix : matrices){
        auto error = matrix.key().get(key);
        if (error) { throw InputException("Error while parsing matrixes."); }
        if (key == "durations") {

        } else if (key == "distances") {
          
        } else if (key == "costs") {

        }
      }
    } else if (key == "matrix") {
      simdjson::ondemand::array array;
      error = field.value().get_array().get(array);
      if (error) { throw InputException("Error while parsing matrix."); }
      for (simdjson::ondemand::array row : array){

      }
    }  
  }
}

} // namespace vroom::io
