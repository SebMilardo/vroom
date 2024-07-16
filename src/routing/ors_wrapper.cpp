/*

This file is part of VROOM.

Copyright (c) 2015-2024, Julien Coupey.
All rights reserved (see LICENSE).

*/

#include "routing/ors_wrapper.h"
#include "utils/helpers.h"

namespace vroom::routing {

OrsWrapper::OrsWrapper(const std::string& profile, const Server& server)
  : HttpWrapper(profile,
                server,
                "matrix",
                "durations",
                "distances",
                "directions",
                "\"geometry_simplify\":\"false\",\"continue_straight\":"
                "\"false\"") {
}

std::string OrsWrapper::build_query(const std::vector<Location>& locations,
                                    const std::string& service) const {
  // Adding locations.
  std::string body = "{\"";
  if (service == "directions") {
    body += "coordinates";
  } else {
    body += "locations";
  }
  body += "\":[";
  for (auto const& location : locations) {
    body += std::format("[{},{}],", location.lon(), location.lat());
  }
  body.pop_back(); // Remove trailing ','.
  body += "]";
  if (service == _route_service) {
    body += "," + _routing_args;
  } else {
    assert(service == _matrix_service);
    body += ",\"metrics\":[\"duration\",\"distance\"]";
  }
  body += "}";

  // Building query for ORS
  std::string query = "POST /" + _server.path + service + "/" + profile;

  query += " HTTP/1.0\r\n";
  query += "Accept: */*\r\n";
  query += "Content-Type: application/json\r\n";
  query += std::format("Content-Length: {}\r\n", body.size());
  query += "Host: " + _server.host + ":" + _server.port + "\r\n";
  query += "Connection: close\r\n";
  query += "\r\n" + body;

  return query;
}

void OrsWrapper::check_response(simdjson::ondemand::value json_result,
                                const std::vector<Location>&,
                                const std::string&) const {
  if (json_result.find_field_unordered("error")) {
    throw RoutingException(
      std::string(json_result.at_path("error.message").get_string().value()));
  }
}

bool OrsWrapper::duration_value_is_null(
  simdjson::ondemand::value matrix_entry) const {
  return matrix_entry.is_null();
}

bool OrsWrapper::distance_value_is_null(
  simdjson::ondemand::value matrix_entry) const {
  return matrix_entry.is_null();
}

UserDuration
OrsWrapper::get_duration_value(simdjson::ondemand::value matrix_entry) const {
  return utils::round<UserDuration>(matrix_entry.get_double());
}

UserDistance
OrsWrapper::get_distance_value(simdjson::ondemand::value matrix_entry) const {
  return utils::round<UserDistance>(matrix_entry.get_double());
}

unsigned OrsWrapper::get_legs_number(simdjson::ondemand::value result) const {
  simdjson::ondemand::array array = result.at_path(".routes[0].segments").get_array();
  return array.count_elements();
}

std::string OrsWrapper::get_geometry(simdjson::ondemand::value result) const {
  return std::string(result.at_path(".routes[0].geometry").get_string().value());
}

} // namespace vroom::routing
