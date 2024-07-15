/*

This file is part of VROOM.

Copyright (c) 2015-2024, Julien Coupey.
All rights reserved (see LICENSE).

*/

#define NDEBUG
#define SIMDJSON_DEVELOPMENT_CHECKS 0

#include <algorithm>

#include "../include/simdjson/singleheader/simdjson.h"

#include "utils/input_parser.h"

namespace vroom::io {

// Helper to get optional array of coordinates.
inline Coordinates parse_coordinates(simdjson::ondemand::value object,
                                     const char* key) {
  double coords[2];
  size_t i = 0;
  try {
    for (double elem : object.get_array()) {
      coords[i++] = elem;
    }
    return {coords[0], coords[1]};
  } catch (const std::exception& e) {
    throw InputException("Invalid " + std::string(key) + " array.");
  }
}

inline std::string get_string(simdjson::ondemand::value json_value,
                              const char* key) {
  std::string value;
  try {
    json_value.get_string(value);
  } catch (const std::exception& e) {
    throw InputException("Invalid " + std::string(key) + " value.");
  }
  return value;
}

inline double get_double(simdjson::ondemand::value json_value,
                         const char* key) {
  double value = 1.;
  try {
    value = json_value.get_double();
  } catch (const std::exception& e) {
    throw InputException("Invalid " + std::string(key) + " value.");
  }
  return value;
}

inline Amount get_amount(simdjson::ondemand::value json_value,
                         const char* key,
                         unsigned amount_size) {
  // Default to zero amount with provided size.
  Amount amount(amount_size);
  size_t i = 0;
  try {
    for (uint64_t element : json_value.get_array()) {
      amount[i++] = element;
      if (i > amount_size) {
        break;
      }
    }
    if (i != amount_size) {
      throw InputException(
        std::format("Inconsistent {} length: {} and {}.", key, i, amount_size));
    }
  } catch (const std::exception& e) {
    throw InputException("Invalid " + std::string(key) + " array.");
  }
  return amount;
}

inline Skills get_skills(simdjson::ondemand::value json_value) {
  Skills skills;
  try {
    for (uint64_t skill : json_value.get_array()) {
      try {
        skills.insert(skill);
      } catch (const std::exception& e) {
        throw InputException("Invalid skill value.");
      }
    }

  } catch (const std::exception& e) {
    throw InputException("Invalid skills object.");
  }
  return skills;
}

inline UserDuration get_duration(simdjson::ondemand::value json_value,
                                 const char* key) {
  UserDuration duration = 0;
  try {
    duration = json_value.get_uint64();
  } catch (const std::exception& e) {
    throw InputException("Invalid " + std::string(key) + " duration.");
  }
  return duration;
}

inline Priority get_priority(simdjson::ondemand::value json_value) {
  Priority priority = 0;
  try {
    priority = json_value.get_uint64();
  } catch (const std::exception& e) {
    throw InputException("Invalid priority value.");
  }
  return priority;
}

template <typename T>
inline std::optional<T> get_value_for(simdjson::ondemand::value json_value,
                                      const char* key) {
  std::optional<T> value;
  try {
    value = json_value.get_uint64();
  } catch (const std::exception& e) {
    throw InputException("Invalid " + std::string(key) + " value.");
  }
  return value;
}

inline TimeWindow get_time_window(simdjson::ondemand::value tw) {
  UserDuration window[2];
  size_t i = 0;
  try {
    for (uint64_t elem : tw.get_array()) {
      window[i++] = elem;
    }
    return {window[0], window[1]};
  } catch (const std::exception& e) {
    throw InputException("Invalid time-window.");
  }
}

inline std::vector<TimeWindow> get_time_windows(simdjson::ondemand::value o) {
  std::vector<TimeWindow> tws;
  simdjson::ondemand::array array;
  auto error = o.get_array().get(array);
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

inline Break get_break(simdjson::ondemand::object o, unsigned amount_size) {
  std::optional<Amount> max_load;
  std::optional<Id> b_id;
  std::vector<TimeWindow> tws;
  UserDuration service = 0;
  std::string description;

  for (auto member : o) {
    auto key = member.key().value();
    auto value = member.value().value();

    if (key == "max_load") {
      max_load = get_amount(value, "max_load", amount_size);
    } else if (key == "id") {
      b_id = value.get_uint64();
    } else if (key == "time_windows") {
      tws = get_time_windows(value);
    } else if (key == "service") {
      service = get_duration(value, "service");
    } else if (key == "description") {
      description = get_string(value, "description");
    }
  }
  if (b_id) {
    return Break(b_id.value(), tws, service, description, max_load);
  } else {
    throw InputException("Invalid or missing id for break.");
  }
}

inline std::vector<Break> get_vehicle_breaks(simdjson::ondemand::object v,
                                             unsigned amount_size) {
  std::vector<Break> breaks;
  for (auto b : v) {
    breaks.push_back(get_break(b.value().get_object(), amount_size));
  }

  std::ranges::sort(breaks, [](const auto& a, const auto& b) {
    return a.tws[0].start < b.tws[0].start ||
           (a.tws[0].start == b.tws[0].start && a.tws[0].end < b.tws[0].end);
  });

  return breaks;
}

inline std::vector<VehicleStep> get_vehicle_steps(simdjson::ondemand::value v,
                                                  Id v_id) {
  std::vector<VehicleStep> steps;
  simdjson::ondemand::array array = v.get_array();

  for (auto a : array) {
    std::optional<UserDuration> at;
    std::optional<UserDuration> after;
    std::optional<UserDuration> before;
    std::string type_str;
    std::optional<Id> s_id;

    auto step = a.get_object();
    for (auto member : step) {
      auto key = member.key().value();
      auto value = member.value().value();

      if (key == "service_at") {
        at = value.get_uint64();
      } else if (key == "service_after") {
        after = value.get_uint64();
      } else if (key == "service_before") {
        before = value.get_uint64();
      } else if (key == "type") {
        type_str = get_string(value, "type");
      } else if (key == "id") {
        s_id = value.get_uint64();
      }
    }

    ForcedService forced_service(at, after, before);
    if (type_str == "start") {
      steps.emplace_back(STEP_TYPE::START, std::move(forced_service));
      continue;
    } else if (type_str == "end") {
      steps.emplace_back(STEP_TYPE::END, std::move(forced_service));
      continue;
    }

    if (s_id) {
      if (type_str == "job") {
        steps.emplace_back(JOB_TYPE::SINGLE, s_id.value(), std::move(forced_service));
      } else if (type_str == "pickup") {
        steps.emplace_back(JOB_TYPE::PICKUP, s_id.value(), std::move(forced_service));
      } else if (type_str == "delivery") {
        steps.emplace_back(JOB_TYPE::DELIVERY, s_id.value(), std::move(forced_service));
      } else if (type_str == "break") {
        steps.emplace_back(STEP_TYPE::BREAK, s_id.value(), std::move(forced_service));
      } else {
        throw InputException(
          std::format("Invalid type in steps for vehicle {}.", v_id));
      }
    } else {
      throw InputException(std::format("Invalid id in steps for vehicle {}.", v_id));
    }
  }

  return steps;
}

inline std::optional<Location>
get_location(const std::optional<Coordinates>& coordinates,
             const std::optional<Index>& index) {
  std::optional<Location> location;
  if (index) {
    // Custom provided matrices and index.
    if (coordinates) {
      location = Location({index.value(), coordinates.value()});
    } else {
      location = Location(index.value());
    }
  } else {
    if (coordinates) {
      location = Location(coordinates.value());
    }
  }
  return location;
}

inline Vehicle get_vehicle(simdjson::ondemand::object json_vehicle,
                           unsigned amount_size) {
  Id v_id;
  std::optional<Coordinates> start_coordinates;
  std::optional<Index> start_index;
  std::optional<Coordinates> end_coordinates;
  std::optional<Index> end_index;
  std::string profile = DEFAULT_PROFILE;
  Amount capacity(amount_size);
  Skills skills;
  TimeWindow tw;
  std::vector<Break> breaks;
  std::string description;
  double speed_factor = 1.0;
  std::optional<size_t> max_tasks;
  std::optional<UserDuration> max_travel_time;
  std::optional<UserDuration> max_distance;
  std::vector<VehicleStep> steps;
  UserCost fixed = 0;
  UserCost per_hour = DEFAULT_COST_PER_HOUR;
  UserCost per_km = DEFAULT_COST_PER_KM;

  for (auto member : json_vehicle) {
    auto key = member.key().value();
    auto value = member.value().value();
    if (key == "id") {
      v_id = value.get_uint64();
    } else if (key == "start") {
      start_coordinates = parse_coordinates(value, "start");
    } else if (key == "start_index") {
      start_index = value.get_uint64();
    } else if (key == "end") {
      end_coordinates = parse_coordinates(value, "end");
    } else if (key == "end_index") {
      end_index = value.get_uint64();
    } else if (key == "profile") {
      profile = get_string(value, "profile");
    } else if (key == "capacity") {
      capacity = get_amount(value, "capacity", amount_size);
    } else if (key == "skills") {
      skills = get_skills(value);
    } else if (key == "tw") {
      tw = get_time_window(value);
    } else if (key == "breaks") {
      breaks = get_vehicle_breaks(value, amount_size);
    } else if (key == "description") {
      description = get_string(value, "description");
    } else if (key == "cost") {
      for (auto cost : value.get_object()) {
        if (cost.key() == "fixed") {
          fixed = cost.value().get_uint64();
        }
        if (cost.key() == "per_hour") {
          per_hour = cost.value().get_uint64();
        }
        if (cost.key() == "per_km") {
          per_km = cost.value().get_uint64();
        }
      }
    } else if (key == "speed_factor") {
      speed_factor = get_double(value, "speed_factor");
    } else if (key == "max_tasks") {
      max_tasks = get_value_for<size_t>(value, "max_tasks");
    } else if (key == "max_travel_time") {
      max_travel_time = get_value_for<UserDuration>(value, "max_travel_time");
    } else if (key == "max_distance") {
      max_distance = get_value_for<UserDistance>(value, "max_distance");
    } else if (key == "steps") {
      steps = get_vehicle_steps(value, v_id);
    }
  }

  std::optional<Location> start = get_location(start_coordinates, start_index);
  std::optional<Location> end = get_location(end_coordinates, end_index);

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
                 steps);
}

inline Job get_job(simdjson::ondemand::object json_job, unsigned amount_size) {
  // Only for retro-compatibility: when no pickup and delivery keys
  // are defined and (deprecated) amount key is present, it should be
  // interpreted as a delivery.

  std::optional<Id> j_id;
  std::optional<Coordinates> location_coordinates;
  std::optional<Index> location_index;
  UserDuration setup = 0;
  UserDuration service = 0;
  Amount delivery(amount_size);
  Amount pickup(amount_size);
  Skills skills;
  Priority priority = 0;
  std::vector<TimeWindow> tws = std::vector<TimeWindow>(1, TimeWindow());
  std::string description;

  for (auto member : json_job) {
    auto key = member.key().value();
    auto value = member.value().value();

    if (key == "id") {
      j_id = member.value().get_uint64();
    } else if (key == "location_index") {
      location_index = member.value().get_uint64();
    } else if (key == "location") {
      location_coordinates = parse_coordinates(value, "location");
    } else if (key == "setup") {
      setup = get_duration(value, "setup");
    } else if (key == "service") {
      service = get_duration(value, "service");
    } else if (key == "delivery") {
      delivery = get_amount(value, "delivery", amount_size);
    } else if (key == "amount") {
      delivery = get_amount(value, "amount", amount_size);
    } else if (key == "pickup") {
      pickup = get_amount(value, "pickup", amount_size);
    } else if (key == "skills") {
      skills = get_skills(value);
    } else if (key == "priority") {
      priority = get_priority(value);
    } else if (key == "time_windows") {
      tws = get_time_windows(value);
    } else if (key == "description") {
      description = get_string(value, "description");
    }
  }

  std::optional<Location> location =
    get_location(location_coordinates, location_index);

  if (j_id) {
    return Job(j_id.value(),
               location.value(),
               setup,
               service,
               delivery,
               pickup,
               skills,
               priority,
               tws,
               description);
  } else {
    throw InputException("Invalid or missing id for job.");
  }
}

template <class T>
inline Matrix<T> get_matrix(simdjson::ondemand::array array) {
  // Load custom matrix while checking if it is square.
  unsigned int matrix_size = array.count_elements();

  Matrix<T> matrix(matrix_size);
  size_t i = 0;
  size_t j = 0;
  for (simdjson::ondemand::array sub_array : array) {
    j = 0;
    for (uint64_t element : sub_array) {
      matrix[i][j++] = element;
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

  size_t amount_size = 0;
  simdjson::ondemand::array first_capacity;
  auto error = doc.at_path(".vehicles[0].capacity").get(first_capacity);
  if (!error)
    amount_size = first_capacity.count_elements();
  std::cout << "amount_size " << amount_size << std::endl;
  doc.rewind();
  input.set_amount_size(amount_size);
  input.set_geometry(geometry);

  simdjson::ondemand::object obj;
  error = doc.get_object().get(obj);
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
    std::cout << key << std::endl;
    if (key == "jobs") {
      simdjson::ondemand::array jobs;
      error = field.value().get_array().get(jobs);
      if (error) {
        throw InputException("Error while parsing jobs.");
      }
      for (simdjson::ondemand::object job : jobs) {
        input.add_job(get_job(job, amount_size));
      }
    } else if (key == "shipments") {
      simdjson::ondemand::array shipments;
      error = field.value().get_array().get(shipments);
      if (error) {
        throw InputException("Error while parsing shipments.");
      }
      for (simdjson::ondemand::object shipment : shipments) {
        Amount amount(amount_size);
        Skills skills;
        Priority priority = 0;

        std::optional<Id> pickup_id;
        std::optional<Coordinates> pickup_location_coordinates;
        std::optional<Index> pickup_location_index;
        UserDuration pickup_setup = 0;
        UserDuration pickup_service = 0;
        std::vector<TimeWindow> pickup_tws =
          std::vector<TimeWindow>(1, TimeWindow());
        std::string pickup_description;

        std::optional<Id> delivery_id;
        std::optional<Coordinates> delivery_location_coordinates;
        std::optional<Index> delivery_location_index;
        UserDuration delivery_setup = 0;
        UserDuration delivery_service = 0;
        std::vector<TimeWindow> delivery_tws =
          std::vector<TimeWindow>(1, TimeWindow());
        std::string delivery_description;

        for (auto member : shipment) {
          auto key = member.key().value();
          auto value = member.value().value();
          if (key == "pickup") {
            simdjson::ondemand::object pickup = member.value().get_object();
            for (auto submember : pickup) {
              auto subkey = submember.key().value();
              auto subvalue = submember.value().value();
              if (subkey == "id") {
                pickup_id = subvalue.get_uint64();
              } else if (subkey == "setup") {
                pickup_setup = get_duration(subvalue, "setup");
              } else if (subkey == "service") {
                pickup_service = get_duration(subvalue, "service");
              } else if (subkey == "time_windows") {
                pickup_tws = get_time_windows(subvalue);
              } else if (subkey == "location_index") {
                pickup_location_index = subvalue.get_uint64();
              } else if (subkey == "location") {
                pickup_location_coordinates =
                  parse_coordinates(subvalue, "location");
              } else if (subkey == "description") {
                pickup_description = get_string(subvalue, "description");
              }
            }

          } else if (key == "delivery") {
            simdjson::ondemand::object delivery = member.value().get_object();
            for (auto submember : delivery) {
              auto subkey = submember.key().value();
              auto subvalue = submember.value().value();
              if (subkey == "id") {
                delivery_id = subvalue.get_uint64();
              } else if (subkey == "setup") {
                delivery_setup = get_duration(subvalue, "setup");
              } else if (subkey == "service") {
                delivery_service = get_duration(subvalue, "service");
              } else if (subkey == "time_windows") {
                delivery_tws = get_time_windows(subvalue);
              } else if (subkey == "location_index") {
                delivery_location_index = subvalue.get_uint64();
              } else if (subkey == "location") {
                delivery_location_coordinates =
                  parse_coordinates(subvalue, "location");
              } else if (subkey == "description") {
                delivery_description = get_string(subvalue, "description");
              }
            }
          } else if (key == "amount") {
            amount = get_amount(value, "amount", amount_size);
          } else if (key == "skills") {
            skills = get_skills(value);
          } else if (key == "priority") {
            priority = get_priority(value);
          }
        }

        std::optional<Location> delivery_location =
          get_location(delivery_location_coordinates, delivery_location_index);
        std::optional<Location> pickup_location =
          get_location(pickup_location_coordinates, pickup_location_index);
        ;

        if (!pickup_id)
          throw InputException("Invalid or missing id for pickup.");
        if (!delivery_id)
          throw InputException("Invalid or missing id for delivery.");

        Job pickup(pickup_id.value(),
                   JOB_TYPE::PICKUP,
                   pickup_location.value(),
                   pickup_setup,
                   pickup_service,
                   amount,
                   skills,
                   priority,
                   pickup_tws,
                   pickup_description);

        Job delivery(delivery_id.value(),
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
        input.add_vehicle(get_vehicle(vehicle, amount_size));
      }
    } else if (key == "matrices") {
      std::cout << "matrices" << std::endl;
      for (auto profile : field.value().get_object()) {
        auto profile_key = std::string(profile.escaped_key().value());
        std::cout << profile_key << std::endl;
        auto matrices = profile.value().get_object();
        for (auto matrix : matrices) {
          auto error = matrix.key().get(key);
          if (error) {
            throw InputException("Error while parsing matrixes.");
          }
          std::cout << key << std::endl;
          if (key == "durations") {
            input.set_durations_matrix(profile_key,
                                       get_matrix<UserDuration>(
                                         matrix.value().get_array()));
          } else if (key == "distances") {
            input.set_distances_matrix(profile_key,
                                       get_matrix<UserDistance>(
                                         matrix.value().get_array()));
          } else if (key == "costs") {
            input.set_costs_matrix(profile_key,
                                   get_matrix<UserCost>(
                                     matrix.value().get_array()));
          }
        }
      }

    } else if (key == "matrix") {
      // Deprecated `matrix` key still interpreted as
      // `matrices.DEFAULT_PROFILE.duration` for retro-compatibility.
      simdjson::ondemand::array array;
      error = field.value().get_array().get(array);
      if (error) {
        throw InputException("Error while parsing matrix.");
      }
      input.set_durations_matrix(DEFAULT_PROFILE,
                                 get_matrix<UserDuration>(array));
    }
  }
}

} // namespace vroom::io
