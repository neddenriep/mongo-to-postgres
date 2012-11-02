Mongo to Postgres Converter

These are two tasks that should serve as starting points for a Mongo to Postgres conversion. You will need to modify both of them to fit some of the unique parts of your schema and data.

As is it should handle polymorphic relationships, habtm, has many, and embeds many.

The tasks assume that models inherit from ActiveRecord::Base and that BSON ids are unique across all collections and embedded collections.

See the SD Ruby talk and slides (To be posted) for more info.