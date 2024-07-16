/*

This file is part of VROOM.

Copyright (c) 2015-2024, Julien Coupey.
All rights reserved (see LICENSE).

*/

#include <utility>

#include <asio.hpp>
#include <asio/ssl.hpp>

#include "routing/http_wrapper.h"

using asio::ip::tcp;

namespace vroom::routing {

const std::string HttpWrapper::HTTPS_PORT = "443";

HttpWrapper::HttpWrapper(const std::string& profile,
                         Server server,
                         std::string matrix_service,
                         std::string matrix_durations_key,
                         std::string matrix_distances_key,
                         std::string route_service,
                         std::string routing_args)
  : Wrapper(profile),
    _server(std::move(server)),
    _matrix_service(std::move(matrix_service)),
    _matrix_durations_key(std::move(matrix_durations_key)),
    _matrix_distances_key(std::move(matrix_distances_key)),
    _route_service(std::move(route_service)),
    _routing_args(std::move(routing_args)) {
}

std::string HttpWrapper::send_then_receive(const std::string& query) const {
  std::string response;

  try {
    asio::io_service io_service;

    tcp::resolver r(io_service);

    tcp::resolver::query q(_server.host, _server.port);

    tcp::socket s(io_service);
    asio::connect(s, r.resolve(q));

    asio::write(s, asio::buffer(query));

    char buf[512]; // NOLINT
    std::error_code error;
    for (;;) {
      std::size_t len = s.read_some(asio::buffer(buf), error);
      response.append(buf, len); // NOLINT
      if (error == asio::error::eof) {
        // Connection closed cleanly.
        break;
      }
      if (error) {
        throw std::system_error(error);
      }
    }
  } catch (std::system_error&) {
    throw RoutingException("Failed to connect to " + _server.host + ":" +
                           _server.port);
  }

  // Removing headers.
  auto start = response.find('{');
  if (start == std::string::npos) {
    throw RoutingException("Invalid routing response: " + response);
  }
  auto end = response.rfind('}');
  if (end == std::string::npos) {
    throw RoutingException("Invalid routing response: " + response);
  }

  std::string json_string = response.substr(start, end - start + 1);

  return json_string;
}

std::string HttpWrapper::ssl_send_then_receive(const std::string& query) const {
  std::string response;

  try {
    asio::io_service io_service;

    asio::ssl::context ctx(asio::ssl::context::method::sslv23_client);
    asio::ssl::stream<asio::ip::tcp::socket> ssock(io_service, ctx);

    tcp::resolver r(io_service);

    tcp::resolver::query q(_server.host, _server.port);

    asio::connect(ssock.lowest_layer(), r.resolve(q));
    ssock.handshake(asio::ssl::stream_base::handshake_type::client);

    asio::write(ssock, asio::buffer(query));

    char buf[512]; // NOLINT
    std::error_code error;
    for (;;) {
      std::size_t len = ssock.read_some(asio::buffer(buf), error);
      response.append(buf, len); // NOLINT
      if (error == asio::error::eof) {
        // Connection closed cleanly.
        break;
      }
      if (error) {
        throw std::system_error(error);
      }
    }
  } catch (std::system_error&) {
    throw RoutingException("Failed to connect to " + _server.host + ":" +
                           _server.port);
  }

  // Removing headers.
  auto start = response.find('{');
  if (start == std::string::npos) {
    throw RoutingException("Invalid routing response: " + response);
  }
  auto end = response.rfind('}');
  if (end == std::string::npos) {
    throw RoutingException("Invalid routing response: " + response);
  }
  std::string json_string = response.substr(start, end - start + 1);

  return json_string;
}

std::string HttpWrapper::run_query(const std::string& query) const {
  return (_server.port == HTTPS_PORT) ? ssl_send_then_receive(query)
                                      : send_then_receive(query);
}

void HttpWrapper::parse_response(simdjson::ondemand::document& json_result,
                                 const std::string& json_content) {

#ifdef NDEBUG
  simdjson::padded_string padded_input_str(json_content);
  simdjson::ondemand::parser parser;
  json_result = parser.iterate(padded_input_str);
#else
  simdjson::padded_string padded_input_str(json_content);
  simdjson::ondemand::parser parser;
  json_result = parser.iterate(padded_input_str);
#endif
}

Matrices HttpWrapper::get_matrices(const std::vector<Location>& locs) const {
  std::string query = this->build_query(locs, _matrix_service);
  std::string json_string = this->run_query(query);

  // Expected matrix size.
  std::size_t m_size = locs.size();

  simdjson::ondemand::document json_result;
  this->parse_response(json_result, json_string);
  this->check_response(json_result, locs, _matrix_service);

  std::optional<simdjson::ondemand::array> durations_matrix;
  std::optional<simdjson::ondemand::array> distances_matrix;

  std::vector<unsigned> nb_unfound_from_loc(m_size, 0);
  std::vector<unsigned> nb_unfound_to_loc(m_size, 0);

  // Build matrices while checking for unfound routes ('null' values)
  // to avoid unexpected behavior.
  Matrices m(m_size);

  for (auto field : json_result.get_object()){
    if (field.key() == _matrix_durations_key){
      durations_matrix = field.value().get_array();
      size_t i = 0;
      for (simdjson::ondemand::array line : durations_matrix.value()){
        size_t j = 0;
        for (simdjson::ondemand::value value : line){
          if (duration_value_is_null(value)){
            ++nb_unfound_from_loc[i];
            ++nb_unfound_to_loc[j];        
          } else {
            m.durations[i][j] = get_duration_value(value);
          }
          ++j;
        }
        assert(j == m_size);
        ++i;
      }
      assert(i == m_size);
    } else if (field.key() == _matrix_distances_key){
      distances_matrix = field.value().get_array();
      size_t i = 0;
      for (simdjson::ondemand::array line : distances_matrix.value()){
        size_t j = 0;
        for (simdjson::ondemand::value value : line){
          if (distance_value_is_null(value)){
            ++nb_unfound_from_loc[i];
            ++nb_unfound_to_loc[j];        
          } else {
            m.distances[i][j] = get_distance_value(value);
          }
          ++j;
        }
        assert(j == m_size);
        ++i;
      }
      assert(i == m_size);
    }
  }
  if (!durations_matrix) {
    throw RoutingException("Missing " + _matrix_durations_key + ".");
  }
  if (!distances_matrix) {
    throw RoutingException("Missing " + _matrix_distances_key + ".");
  }

  check_unfound(locs, nb_unfound_from_loc, nb_unfound_to_loc);
  return m;
}

void HttpWrapper::add_geometry(Route& route) const {
  // Ordering locations for the given steps, excluding
  // breaks.
  std::vector<Location> non_break_locations;
  non_break_locations.reserve(route.steps.size());

  for (const auto& step : route.steps) {
    if (step.step_type != STEP_TYPE::BREAK) {
      assert(step.location.has_value());
      non_break_locations.push_back(step.location.value());
    }
  }
  assert(!non_break_locations.empty());

  std::string query = build_query(non_break_locations, _route_service);

  std::string json_string = this->run_query(query);

  simdjson::ondemand::document json_result;
  parse_response(json_result, json_string);
  this->check_response(json_result,
                       non_break_locations, // not supposed to be used
                       _route_service);

  assert(get_legs_number(json_result) == non_break_locations.size() - 1);

  route.geometry = get_geometry(json_result);
}

} // namespace vroom::routing
