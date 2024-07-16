/*

This file is part of VROOM.

Copyright (c) 2015-2024, Julien Coupey.
All rights reserved (see LICENSE).

*/

#include "routing/osrm_routed_wrapper.h"
#include "utils/helpers.h"

namespace vroom::routing {

OsrmRoutedWrapper::OsrmRoutedWrapper(const std::string& profile,
                                     const Server& server)
  : HttpWrapper(profile,
                server,
                "table",
                "durations",
                "distances",
                "route",
                "alternatives=false&steps=false&overview=full&continue_"
                "straight=false") {
}

std::string
OsrmRoutedWrapper::build_query(const std::vector<Location>& locations,
                               const std::string& service) const {
  // Building query for osrm-routed
  std::string query = "GET /" + _server.path + service;

  query += "/v1/" + profile + "/";

  // Build query part for snapping restriction.
  std::string radiuses = "radiuses=";
  radiuses.reserve(9 + locations.size() *
                         (DEFAULT_OSRM_SNAPPING_RADIUS.size() + 1));

  // Adding locations and radiuses values.
  for (auto const& location : locations) {
    query += std::format("{},{};", location.lon(), location.lat());
    radiuses += DEFAULT_OSRM_SNAPPING_RADIUS + ";";
  }
  // Remove trailing ';'.
  query.pop_back();
  radiuses.pop_back();

  if (service == _route_service) {
    query += "?" + _routing_args;
  } else {
    assert(service == _matrix_service);
    query += "?annotations=duration,distance";
  }
  query += "&" + radiuses;

  query += " HTTP/1.1\r\n";
  query += "Host: " + _server.host + "\r\n";
  query += "Accept: */*\r\n";
  query += "Connection: close\r\n\r\n";

  return query;
}

void OsrmRoutedWrapper::check_response(simdjson::ondemand::value json_result,
                                       const std::vector<Location>& locs,
                                       const std::string&) const {
  assert(json_result.find_field_unordered("code"));
  std::string code;
  json_result["code"].get_string(code);
  if (code != "Ok") {
    std::string message;
    json_result["message"].get_string(message);
    if (const std::string snapping_error_base =
          "Could not find a matching segment for coordinate ";
        code == "NoSegment" && message.starts_with(snapping_error_base)) {
      const auto error_loc =
        std::stoul(message.substr(snapping_error_base.size(),
                                  message.size() - snapping_error_base.size()));
      const auto coordinates =
        std::format("[{},{}]", locs[error_loc].lon(), locs[error_loc].lat());
      throw RoutingException("Could not find route near location " +
                             coordinates);
    }

    // Other error in response.
    throw RoutingException(message);
  }
}

bool OsrmRoutedWrapper::duration_value_is_null(
  simdjson::ondemand::value matrix_entry) const {
  return matrix_entry.is_null();
}

bool OsrmRoutedWrapper::distance_value_is_null(
  simdjson::ondemand::value matrix_entry) const {
  return matrix_entry.is_null();
}

UserDuration OsrmRoutedWrapper::get_duration_value(
  simdjson::ondemand::value matrix_entry) const {
  return utils::round<UserDuration>(matrix_entry.get_double());
}

UserDistance OsrmRoutedWrapper::get_distance_value(
  simdjson::ondemand::value matrix_entry) const {
  return utils::round<UserDistance>(matrix_entry.get_double());
}

unsigned
OsrmRoutedWrapper::get_legs_number(simdjson::ondemand::value result) const {
  simdjson::ondemand::array array = result["routes"][0]["legs"].get_array();
  return array.count_elements();
}

std::string OsrmRoutedWrapper::get_geometry(simdjson::ondemand::value result) const {
  std::string geometry;
  result["routes"][0]["geometry"].get_string(geometry);
  return geometry;
}

} // namespace vroom::routing
