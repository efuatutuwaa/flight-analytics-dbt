connection: "databricks"

include: "/views/*.view.lkml"

# -------------------------
# Explores
# -------------------------
# An Explore is what a business user sees in Looker.
# It defines which view is queryable and how views join together.

explore: mart_airline_performance {
  label: "Airline Performance"
  description: "Analyse flight volumes, on-time rates, and cancellations by airline."
}

explore: mart_route_performance {
  label: "Route Performance"
  description: "Analyse route efficiency, delay patterns, and cancellations by route."
}

explore: mart_airport_operations {
  label: "Airport Operations"
  description: "Analyse airport traffic volumes and departure/arrival performance."
}
