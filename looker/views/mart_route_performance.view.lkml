view: mart_route_performance {
  sql_table_name: `workspace`.`mart_models`.`mart_route_performance` ;;

  # -------------------------
  # Dimensions
  # -------------------------

  dimension: route_key {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.route_key ;;
  }

  dimension: route_label {
    type: string
    label: "Route"
    description: "Human-readable route label e.g. LHR → JFK."
    sql: ${TABLE}.route_label ;;
  }

  dimension: is_international {
    type: yesno
    label: "International Route"
    sql: ${TABLE}.is_international ;;
  }

  dimension: route_distance_km {
    type: number
    label: "Route Distance (km)"
    sql: ${TABLE}.route_distance_km ;;
  }

  # -------------------------
  # Measures
  # -------------------------

  measure: total_flights {
    type: sum
    label: "Total Flights"
    sql: ${TABLE}.total_flights ;;
  }

  measure: cancelled_flights {
    type: sum
    label: "Cancelled Flights"
    sql: ${TABLE}.cancelled_flights ;;
  }

  measure: airlines_competing {
    type: sum
    label: "Airlines Competing"
    description: "Number of distinct airlines operating this route."
    sql: ${TABLE}.airlines_competing ;;
  }

  measure: on_time_departures {
    type: sum
    label: "On-Time Departures"
    sql: ${TABLE}.on_time_departures ;;
  }

  measure: delayed_departures {
    type: sum
    label: "Delayed Departures"
    sql: ${TABLE}.delayed_departures ;;
  }

  measure: on_time_arrivals {
    type: sum
    label: "On-Time Arrivals"
    sql: ${TABLE}.on_time_arrivals ;;
  }

  measure: delayed_arrivals {
    type: sum
    label: "Delayed Arrivals"
    sql: ${TABLE}.delayed_arrivals ;;
  }

  measure: cancellation_rate {
    type: number
    label: "Cancellation Rate"
    description: "Proportion of flights cancelled. Computed from components for accuracy."
    sql: safe_divide(sum(${TABLE}.cancelled_flights), sum(${TABLE}.total_flights)) ;;
    value_format_name: percent_2
  }

  measure: on_time_departure_rate {
    type: number
    label: "On-Time Departure Rate"
    description: "Proportion of non-cancelled flights that departed on time (< 15 min delay)."
    sql: safe_divide(sum(${TABLE}.on_time_departures), sum(${TABLE}.on_time_departures) + sum(${TABLE}.delayed_departures)) ;;
    value_format_name: percent_2
  }

  measure: on_time_arrival_rate {
    type: number
    label: "On-Time Arrival Rate"
    description: "Proportion of non-cancelled flights that arrived on time (< 15 min delay)."
    sql: safe_divide(sum(${TABLE}.on_time_arrivals), sum(${TABLE}.on_time_arrivals) + sum(${TABLE}.delayed_arrivals)) ;;
    value_format_name: percent_2
  }

  measure: avg_departure_delay_minutes {
    type: average
    label: "Avg Departure Delay (mins)"
    sql: ${TABLE}.avg_departure_delay_minutes ;;
    value_format_name: decimal_1
  }

  measure: avg_arrival_delay_minutes {
    type: average
    label: "Avg Arrival Delay (mins)"
    sql: ${TABLE}.avg_arrival_delay_minutes ;;
    value_format_name: decimal_1
  }
}
