view: mart_airport_operations {
  sql_table_name: `workspace`.`mart_models`.`mart_airport_operations` ;;

  # -------------------------
  # Dimensions
  # -------------------------

  dimension: airport_id {
    type: string
    primary_key: yes
    hidden: yes
    sql: ${TABLE}.airport_id ;;
  }

  dimension: airport_name {
    type: string
    label: "Airport Name"
    sql: ${TABLE}.airport_name ;;
  }

  dimension: airport_iata_code {
    type: string
    label: "IATA Code"
    sql: ${TABLE}.airport_iata_code ;;
  }

  dimension: city_name {
    type: string
    label: "City"
    sql: ${TABLE}.city_name ;;
  }

  dimension: country_name {
    type: string
    label: "Country"
    sql: ${TABLE}.country_name ;;
  }

  dimension: country_continent {
    type: string
    label: "Continent"
    sql: ${TABLE}.country_continent ;;
  }

  dimension: latitude {
    type: number
    hidden: yes
    sql: ${TABLE}.latitude ;;
  }

  dimension: longitude {
    type: number
    hidden: yes
    sql: ${TABLE}.longitude ;;
  }

  # Map location — combines lat/lon for Looker map visualisations
  dimension: location {
    type: location
    label: "Airport Location"
    sql_latitude: ${TABLE}.latitude ;;
    sql_longitude: ${TABLE}.longitude ;;
  }

  # -------------------------
  # Measures
  # -------------------------

  measure: total_departures {
    type: sum
    label: "Total Departures"
    sql: ${TABLE}.total_departures ;;
  }

  measure: total_arrivals {
    type: sum
    label: "Total Arrivals"
    sql: ${TABLE}.total_arrivals ;;
  }

  measure: total_movements {
    type: sum
    label: "Total Movements"
    description: "Total departures and arrivals combined."
    sql: ${TABLE}.total_movements ;;
  }

  measure: cancelled_departures {
    type: sum
    label: "Cancelled Departures"
    sql: ${TABLE}.cancelled_departures ;;
  }

  measure: cancelled_arrivals {
    type: sum
    label: "Cancelled Arrivals"
    sql: ${TABLE}.cancelled_arrivals ;;
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
