/*

This file is part of VROOM.

Copyright (c) 2015-2024, Julien Coupey.
All rights reserved (see LICENSE).

*/

#include <algorithm>

#include "../include/simdjson/singleheader/simdjson.h"

#include "utils/input_parser.h"

namespace vroom::io {

inline std::optional<simdjson::ondemand::value>
HasMember(simdjson::ondemand::object object, const char* key) {
  try {
    return object[key];
  } catch (const std::exception& e) {
    return std::nullopt;
  }
}

// Helper to get optional array of coordinates.
inline Coordinates parse_coordinates(simdjson::ondemand::object object,
                                     const char* key) {
  auto member = HasMember(object, key);
  if (member) {
    try {
      simdjson::ondemand::array array = member.value().get_array();
      return {array.at(0).get_double(), array.at(1).get_double()};
    } catch (const std::exception& e) {
      throw InputException("Invalid " + std::string(key) + " array.");
    }
  }
}

inline std::string get_string(simdjson::ondemand::object object,
                              const char* key) {
  std::string value;
  auto member = HasMember(object, key);
  if (member) {
    try {
      member.value().get_string(value);
    } catch (const std::exception& e) {
      throw InputException("Invalid " + std::string(key) + " value.");
    }
  }
  return value;
}

inline double get_double(simdjson::ondemand::object object, const char* key) {
  double value = 1.;
  auto member = HasMember(object, key);
  if (member) {
    try {
      value = member.value().get_double();
    } catch (const std::exception& e) {
      throw InputException("Invalid " + std::string(key) + " value.");
    }
  }
  return value;
}

inline Amount get_amount(simdjson::ondemand::object object,
                         const char* key,
                         unsigned amount_size) {
  // Default to zero amount with provided size.
  Amount amount(amount_size);
  try {
    simdjson::ondemand::array array = object[key].value().get_array();
    uint array_size = array.count_elements();
    if (array_size != amount_size) {
      throw InputException(std::format("Inconsistent {} length: {} and {}.",
                                       key,
                                       array_size,
                                       amount_size));
    }

    for (uint i = 0; i < array_size; ++i) {
      try {
        amount[i] = array.at(i).get_uint64();
      } catch (const std::exception& e) {
        throw InputException("Invalid " + std::string(key) + " value.");
      }
    }
  } catch (const std::exception& e) {
    throw InputException("Invalid " + std::string(key) + " array.");
  }
  return amount;
}

inline Skills get_skills(simdjson::ondemand::object object) {
  Skills skills;
  auto member = HasMember(object, "skills");
  if (member) {
    try {
      simdjson::ondemand::array array = member.value().get_array();

      for (auto skill : array) {
        try {
          skills.insert(skill.get_uint64());
        } catch (const std::exception& e) {
          throw InputException("Invalid skill value.");
        }
      }

    } catch (const std::exception& e) {
      throw InputException("Invalid skills object.");
    }
  }
  return skills;
}

inline UserDuration get_duration(simdjson::ondemand::object object,
                                 const char* key) {
  UserDuration duration = 0;
  auto member = HasMember(object, key);
  if (member) {
    try {
      duration = member.value().get_uint64();
    } catch (const std::exception& e) {
      throw InputException("Invalid " + std::string(key) + " duration.");
    }
  }
  return duration;
}

inline Priority get_priority(simdjson::ondemand::object object) {
  Priority priority = 0;
  auto member = HasMember(object, "priority");
  if (member) {
    try {
      priority = member.value().get_uint64();
    } catch (const std::exception& e) {
      throw InputException("Invalid priority value.");
    }
  }
  return priority;
}

template <typename T>
inline std::optional<T> get_value_for(simdjson::ondemand::object object,
                                      const char* key) {
  std::optional<T> value;
  auto member = HasMember(object, key);
  if (member) {
    try {
      value = member.value().get_uint64();
    } catch (const std::exception& e) {
      throw InputException("Invalid " + std::string(key) + " value.");
    }
  }
  return value;
}

inline void check_id(simdjson::ondemand::object v, const char* type) {
  auto member = HasMember(v, "id");
  if (member) {
    try {
      member.value().get_uint64();
    } catch (const std::exception& e) {
      throw InputException("Invalid id for " + std::string(type) + ".");
    }
  } else {
    throw InputException("Missing id for " + std::string(type) + ".");
  }
}

inline TimeWindow get_time_window(simdjson::ondemand::value tw) {
  try {
    simdjson::ondemand::array array = tw.get_array();
    return {array.at(0).get_uint64(), array.at(1).get_uint64()};
  } catch (const std::exception& e) {
    throw InputException("Invalid time-window.");
  }
}

inline TimeWindow get_vehicle_time_window(simdjson::ondemand::object v) {
  TimeWindow v_tw;
  auto member = HasMember(v, "time_window");
  if (member) {
    v_tw = get_time_window(member.value());
  }
  return v_tw;
}

inline std::vector<TimeWindow> get_time_windows(simdjson::ondemand::object o) {
  std::vector<TimeWindow> tws;
  simdjson::ondemand::array array;
  auto error = o["time_windows"].get_array().get(array);
  if (error || (array.begin() == array.end())) {
    throw InputException(
      std::format("Invalid time_windows array for object {}.",
                  o["id"].get_uint64().value()));
  }
  for (simdjson::ondemand::value tw : array) {
    tws.push_back(get_time_window(tw));
  }

  std::sort(tws.begin(), tws.end());
  return tws;
}

inline Break get_break(simdjson::ondemand::object b, unsigned amount_size) {
  check_id(b, "break");

  const auto max_load = HasMember(b, "max_load")
                          ? get_amount(b, "max_load", amount_size)
                          : std::optional<Amount>();

  return Break(b["id"].get_uint64(),
               get_time_windows(b),
               get_duration(b, "service"),
               get_string(b, "description"),
               max_load);
}

inline std::vector<Break> get_vehicle_breaks(simdjson::ondemand::object v,
                                             unsigned amount_size) {
  std::vector<Break> breaks;
  auto has_breaks = HasMember(v, "breaks");
  if (has_breaks) {
    try {
      for (auto b : has_breaks.value()) {
        breaks.push_back(get_break(b, amount_size));
      }
    } catch (const std::exception& e) {
      throw InputException(std::format("Invalid breaks for vehicle {}.",
                                       v["id"].get_uint64().value()));
    }
  }

  std::ranges::sort(breaks, [](const auto& a, const auto& b) {
    return a.tws[0].start < b.tws[0].start ||
           (a.tws[0].start == b.tws[0].start && a.tws[0].end < b.tws[0].end);
  });

  return breaks;
}

inline Vehicle get_vehicle(simdjson::ondemand::object json_vehicle,
                           unsigned amount_size) {
  uint64_t v_id;
  Coordinates start_coordinates;
  uint64_t start_index;
  Coordinates end_coordinates;
  uint64_t end_index;
  std::string profile;
  Amount capacity((amount_size));
  Skills skills;
  TimeWindow tw;
  std::vector<Break> breaks;
  std::string description;
  double speed_factor = 1.0;
  std::optional<size_t> max_tasks;
  std::optional<UserDuration> max_travel_time;
  std::optional<UserDuration> max_distance;
  std::vector<VehicleStep> steps;
  bool has_start_coords = false;
  bool has_end_coords = false;
  bool has_start_index = false;
  bool has_end_index = false;
  UserCost fixed = 0;
  UserCost per_hour = DEFAULT_COST_PER_HOUR;
  UserCost per_km = DEFAULT_COST_PER_KM;

  for (auto member : json_vehicle) {
    auto key = member.key().value();
    if (key == "id") {
      v_id = member.value().get_uint64();
    } else if (key == "start") {
      start_coordinates = parse_coordinates(json_vehicle, "start");
      has_start_coords = true;
    } else if (key == "start_index") {
      start_index = member.value().get_uint64();
    } else if (key == "end") {
      end_coordinates = parse_coordinates(json_vehicle, "end");
      has_end_coords = true;
    } else if (key == "end_index") {
      end_index = member.value().get_uint64();
    } else if (key == "profile") {
      profile = get_string(json_vehicle, "profile");
      if (profile.empty()) {
        profile = DEFAULT_PROFILE;
      }
    } else if (key == "capacity") {
      capacity = get_amount(json_vehicle, "capacity", amount_size);
    } else if (key == "skills") {
      skills = get_skills(json_vehicle);
    } else if (key == "tw") {
      tw = get_vehicle_time_window(json_vehicle);
    } else if (key == "breaks") {
      breaks = get_vehicle_breaks(json_vehicle, amount_size);
    } else if (key == "description") {
      description = get_string(json_vehicle, "description");
    } else if (key == "cost") {
      for (auto costs : member.value().get_object()) {
        if (costs.key() == "fixed") {
          fixed = member.value().get_uint64();
        }
        if (costs.key() == "per_hour") {
          per_hour = member.value().get_uint64();
        }
        if (costs.key() == "per_km") {
          per_km = member.value().get_uint64();
        }
      }
    } else if (key == "speed_factor") {
      speed_factor = get_double(json_vehicle, "speed_factor");
    } else if (key == "max_tasks") {
      max_tasks = get_value_for<size_t>(json_vehicle, "max_tasks");
    } else if (key == "max_travel_time") {
      max_travel_time =
        get_value_for<UserDuration>(json_vehicle, "max_travel_time");
    } else if (key == "max_distance") {
      max_distance = get_value_for<UserDistance>(json_vehicle, "max_distance");
    } else if (key == "steps") {
      steps = std::vector<VehicleStep>(); // get_vehicle_steps(json_vehicle)
    }
  }

  std::optional<Location> start;
  if (has_start_index) {
    // Custom provided matrices and index.
    if (has_start_coords) {
      start = Location({start_index, start_coordinates});
    } else {
      start = Location(start_index);
    }
  } else {
    if (has_start_coords) {
      start = Location(start_coordinates);
    }
  }

  std::optional<Location> end;
  if (has_end_index) {
    // Custom provided matrices and index.
    if (has_end_coords) {
      end = Location({end_index, end_coordinates});
    } else {
      end = Location(end_index);
    }
  } else {
    if (has_end_coords) {
      end = Location(end_coordinates);
    }
  }

  return Vehicle(v_id,
                 start,
                 end,
                 profile,
                 capacity,
                 skills,
                 tw,
                 breaks,
                 description,
                 VehicleCosts(fixed, per_hour, per_km),
                 speed_factor,
                 max_tasks,
                 max_travel_time,
                 max_distance,
                 steps // get_vehicle_steps(json_vehicle)
  );
}

inline Location get_task_location(simdjson::ondemand::object v,
                                  const std::string& type) {
  // Check what info are available to build task location.
  auto has_location_coords = HasMember(v, "location");
  auto has_location_index = HasMember(v, "location_index");
  if (has_location_index && has_location_index.value().type() !=
                              simdjson::ondemand::json_type::number) {
    throw InputException(std::format("Invalid location_index for {} {}.",
                                     type,
                                     v["id"].get_uint64().value()));
  }

  if (has_location_index) {
    // Custom provided matrices and index.
    Index location_index = has_location_index.value().get_uint64();
    if (has_location_coords) {
      return Location({location_index, parse_coordinates(v, "location")});
    }
    return Location(location_index);
  }
  // check_location(v, type);
  return Location(parse_coordinates(v, "location"));
}

inline Job get_job(simdjson::ondemand::object json_job, unsigned amount_size) {
  // Only for retro-compatibility: when no pickup and delivery keys
  // are defined and (deprecated) amount key is present, it should be
  // interpreted as a delivery.

  uint64_t v_id;
  Coordinates location_coordinates;
  uint64_t location_index;
  UserDuration setup = 0;
  UserDuration service = 0;
  Amount delivery((amount_size));
  Amount pickup((amount_size));
  Skills skills;
  Priority priority = 0;
  std::vector<TimeWindow> tws = std::vector<TimeWindow>(1, TimeWindow());
  std::string description;
  bool has_location_coords = false;
  bool has_location_index = false;

  for (auto member : json_job) {
    auto key = member.key().value();
    if (key == "id") {
      v_id = member.value().get_uint64();
    } else if (key == "location_index") {
      has_location_index = true;
      location_index = member.value().get_uint64();
    } else if (key == "location") {
      has_location_coords = true;
      location_coordinates = parse_coordinates(json_job, "location");
    } else if (key == "setup") {
      setup = get_duration(json_job, "setup");
    } else if (key == "service") {
      service = get_duration(json_job, "service");
    } else if (key == "delivery") {
      delivery = get_amount(json_job, "delivery", amount_size);
    } else if (key == "amount") {
      delivery = get_amount(json_job, "amount", amount_size);
    } else if (key == "pickup") {
      pickup = get_amount(json_job, "pickup", amount_size);
    } else if (key == "skills") {
      skills = get_skills(json_job);
    } else if (key == "priority") {
      priority = get_priority(json_job);
    } else if (key == "time_windows") {
      tws = get_time_windows(json_job);
    } else if (key == "description") {
      description = get_string(json_job, "description");
    }
  }

  std::optional<Location> location;
  if (has_location_index) {
    // Custom provided matrices and index.
    if (has_location_coords) {
      location = Location({location_index, location_coordinates});
    } else {
      location = Location(location_index);
    }
  } else {
    if (has_location_coords) {
      location = Location(location_coordinates);
    }
  }

  return Job(v_id,
             location.value(),
             setup,
             service,
             delivery,
             pickup,
             skills,
             priority,
             tws,
             description);
}

template <class T> inline Matrix<T> get_matrix(simdjson::ondemand::value m) {
  if (m.type() != simdjson::ondemand::json_type::array) {
    throw InputException("Invalid matrix.");
  }
  simdjson::ondemand::array array = m.get_array();

  // Load custom matrix while checking if it is square.
  unsigned int matrix_size = array.count_elements();

  Matrix<T> matrix(matrix_size);
  size_t i = 0;
  size_t j = 0;
  for (simdjson::ondemand::array sub_array : array) {
    for (auto element : sub_array) {
      matrix[i][++j] = element.get_uint64();
      if (j > matrix_size) {
        throw InputException("Unexpected matrix line length.");
      }
    }
    ++i;
  }

  return matrix;
}

void parse(Input& input, const std::string& input_str, bool geometry) {
  // Parse and raise parsing errors
  simdjson::padded_string padded_input_str(input_str);
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc = parser.iterate(padded_input_str);

  input.set_amount_size(1);
  input.set_geometry(geometry);

  simdjson::ondemand::object obj;
  auto error = doc.get_object().get(obj);
  if (error) {
    throw InputException("Error while parsing.");
  }
  // Iterate over the document
  for (auto field : obj) {

    simdjson::ondemand::raw_json_string key;
    auto error = field.key().get(key);
    if (error) {
      throw InputException("Error while parsing.");
    }
    if (key == "jobs") {
      simdjson::ondemand::array jobs;
      error = field.value().get_array().get(jobs);
      if (error) {
        throw InputException("Error while parsing jobs.");
      }
      for (simdjson::ondemand::object job : jobs) {
        input.add_job(get_job(job, 1));
      }
    } else if (key == "shipments") {
      std::cout << "SHIPMENTS:" << std::endl;
      simdjson::ondemand::array shipments;
      error = field.value().get_array().get(shipments);
      if (error) {
        throw InputException("Error while parsing shipments.");
      }
      for (simdjson::ondemand::object shipment : shipments) {
        Amount amount((1));
        Skills skills;
        Priority priority = 0;

        uint64_t pickup_id;
        Coordinates pickup_location_coordinates;
        uint64_t pickup_location_index;
        UserDuration pickup_setup = 0;
        UserDuration pickup_service = 0;
        std::vector<TimeWindow> pickup_tws = std::vector<TimeWindow>(1, TimeWindow());
        std::string pickup_description;
        bool has_pickup_location_coords = false;
        bool has_pickup_location_index = false;

        uint64_t delivery_id;
        Coordinates delivery_location_coordinates;
        uint64_t delivery_location_index;
        UserDuration delivery_setup = 0;
        UserDuration delivery_service = 0;
        std::vector<TimeWindow> delivery_tws = std::vector<TimeWindow>(1, TimeWindow());
        std::string delivery_description;
        bool has_delivery_location_coords = false;
        bool has_delivery_location_index = false;

        std::optional<Job> pickup_job;
        std::optional<Job> delivery_job;

        for (auto member : shipment) {
          auto key = member.key().value();
          if (key == "pickup") {
            simdjson::ondemand::object pickup = member.value().get_object();
            for (auto submember : pickup) {
              auto subkey = member.key().value();
              if (subkey == "id"){
                pickup_id = member.value().get_uint64();
              } else if (subkey == "setup") {
                pickup_setup = get_duration(pickup, "setup");
              } else if (subkey == "service") {
                pickup_service = get_duration(pickup, "service");
              } else if (subkey == "time_windows") {
                pickup_tws = get_time_windows(pickup);
              } else if (key == "location_index") {
                has_pickup_location_index = true;
                pickup_location_index = member.value().get_uint64();
              } else if (key == "location") {
                has_pickup_location_coords = true;
                pickup_location_coordinates = parse_coordinates(pickup, "location");
              } else if (subkey == "description") {
                pickup_description = get_string(pickup, "description");
              }
            }
            
          } else if (key == "delivery") {
            simdjson::ondemand::object delivery = member.value().get_object();
            for (auto submember : delivery) {
              auto subkey = member.key().value();
              if (subkey == "id"){
                delivery_id = member.value().get_uint64();
              } else if (subkey == "setup") {
                delivery_setup = get_duration(pickup, "setup");
              } else if (subkey == "service") {
                delivery_service = get_duration(pickup, "service");
              } else if (subkey == "time_windows") {
                delivery_tws = get_time_windows(pickup);
              } else if (key == "location_index") {
                has_delivery_location_index = true;
                delivery_location_index = member.value().get_uint64();
              } else if (key == "location") {
                has_delivery_location_coords = true;
                delivery_location_coordinates = parse_coordinates(pickup, "location");
              } else if (subkey == "description") {
                delivery_description = get_string(pickup, "description");
              }
            }
          } else if (key == "amount") {
            amount = get_amount(shipment, "amount", 1);
          } else if (key == "skills") {
            skills = get_skills(shipment);
          } else if (key == "priority") {
            priority = get_priority(shipment);
          }
        }

          std::optional<Location> delivery_location;
          if (has_delivery_location_index) {
            // Custom provided matrices and index.
            if (has_delivery_location_coords) {
              delivery_location = Location({delivery_location_index, delivery_location_coordinates});
            } else {
              delivery_location = Location(delivery_location_index);
            }
          } else {
            if (has_delivery_location_coords) {
              delivery_location = Location(delivery_location_coordinates);
            }
          }

          std::optional<Location> pickup_location;
          if (has_pickup_location_index) {
            // Custom provided matrices and index.
            if (has_pickup_location_coords) {
              pickup_location = Location({pickup_location_index, pickup_location_coordinates});
            } else {
              pickup_location = Location(pickup_location_index);
            }
          } else {
            if (has_pickup_location_coords) {
              pickup_location = Location(pickup_location_coordinates);
            }
          }
        
        Job pickup(pickup_id,
                   JOB_TYPE::PICKUP,
                   pickup_location.value(),
                   pickup_setup,
                   pickup_service,
                   amount,
                   skills,
                   priority,
                   pickup_tws,
                   pickup_description);

        auto json_delivery = shipment["delivery"];
        Job delivery(delivery_id,
                     JOB_TYPE::DELIVERY,
                     delivery_location.value(),
                     delivery_setup,
                     delivery_service,
                     amount,
                     skills,
                     priority,
                     delivery_tws,
                     delivery_description);

        input.add_shipment(pickup, delivery);
      }
    } else if (key == "vehicles") {
      simdjson::ondemand::array vehicles;
      error = field.value().get_array().get(vehicles);
      if (error) {
        throw InputException("Error while parsing vehicles.");
      }
      for (simdjson::ondemand::object vehicle : vehicles) {
        input.add_vehicle(get_vehicle(vehicle, 1));
      }
    } else if (key == "matrices") {
      std::cout << "MATRICES:" << std::endl;
      simdjson::ondemand::object matrices;
      error = field.value().get_object().get(matrices);
      if (error) {
        throw InputException("Error while parsing matrices.");
      }
      for (auto matrix : matrices) {
        auto error = matrix.key().get(key);
        if (error) {
          throw InputException("Error while parsing matrixes.");
        }
        if (key == "durations") {

        } else if (key == "distances") {

        } else if (key == "costs") {
        }
      }
    } else if (key == "matrix") {
      simdjson::ondemand::array array;
      error = field.value().get_array().get(array);
      if (error) {
        throw InputException("Error while parsing matrix.");
      }
      for (simdjson::ondemand::array row : array) {
      }
    }
  }
  exit(0);
}

} // namespace vroom::io
