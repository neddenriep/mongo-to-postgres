class Module
  def subclasses
    results = []
    ObjectSpace.each_object do |c|
      next unless Module === c
      results << c if self > c
    end
    results
  end
end

class BSON::OrderedHash
  def to_h
    inject({}) { |acc, element| k,v = element; acc[k] = (if v.class == BSON::OrderedHash then v.to_h else v end); acc }
  end

  def to_json
    to_h.to_json
  end
end

namespace :db do
  task :create_schema => :environment do
    sql_types = {"String"=>"text", "BSON::ObjectId"=>"primary_key", "Time"=>"time", "Object"=>"integer", "Array"=>"text", "Integer"=>"integer", "string"=>"text", "DateTime"=>"datetime", "Date"=>"date", "Hash"=>"text", "Boolean"=>"boolean", "Float"=>"float"}
    serialized = Hash.new
    types = Array.new
    puts "class PostgresCreate < ActiveRecord::Migration"
    puts "  def up"
    Mongoid::Document.subclasses.each do |model|
      serialized[model.name] = Array.new
      puts "    create_table :#{model.name.downcase} do |t|"
      model.fields.each do |field|
        type = field[1].options[:type] || "string"
        types << type
        (serialized[model.name] ||= []) << ":#{field.first.to_sym}" if ["Array", "Hash"].include? type.to_s
        puts "      t.#{sql_types[type.to_s]} :#{field.first.to_sym}"
      end
      puts "    end"
    end
    puts "  end"
    puts "  def down"
    Mongoid::Document.subclasses.each do |model|
      puts "    drop_table :#{model.name.downcase}"
    end
    puts "  end"
    puts "end"
    types.uniq!
    serialized.select {|k,v| !v.blank?}.each {|k,v| puts "#{k}  =>  serialize #{v.join(', ')}"}
  end

  task :export => :environment do |t, args|
    new_sql_line = ""
    @@id_hash = Hash.new
    @@lines = Array.new
    @@id_counter = Hash.new
    @@id_indexed = Hash.new{|h, k| h[k] = []}
    @@all_objects = Hash.new
    @@orphans = Array.new

    Mongoid::Document.subclasses.each do |model|
      # fill this array out for any classes you want to skip
      next if ["Delayed::Backend::Mongoid::Job"].include? model.to_s
      puts "Exporting #{model.to_s}"
      begin
        models = model.all.collect {|m| m}
        generate_from_collection(model, models, nil, nil, nil)
      rescue => e
        puts e
      end
    end

    # gsub all placeholder ids with their replacement postgres ids
    @@id_indexed.each do |key, value|
      replacement = @@id_hash[key]
      if replacement.blank?
        # null out orphan foreign keys
        value.each do |id|
          @@orphans << [@@all_objects[key], key, @@lines[id]]
          pattern = "'#{key}'"
          replacement = "NULL"
          @@lines[id].gsub!(pattern, replacement)
        end
        next
      end
      value.each do |id|
        @@lines[id].gsub!(key, replacement)
      end
    end

    # write out inserts to file
    filename = "#{File.expand_path('~')}/Downloads/mongodump.sql"
    File.open(filename, 'w') do |f|
      @@lines.each do |line|
        f.write(line)
      end
    end
    puts "Exported object count: #{@@id_hash.count}"
  end

  def generate_from_collection(model, models, parent_key, parent_id, poly_in)
    model_table = model.to_s.gsub(/::/, "_").tableize
    insert_string = "INSERT INTO " << model_table << "("
    sql_types = {"String"=>"text", "BSON::ObjectId"=>"primary_key", "Time"=>"time", "Object"=>"integer", "Array"=>"text", "Integer"=>"integer", "string"=>"text", "DateTime"=>"datetime", "Date"=>"date", "Hash"=>"text", "Boolean"=>"boolean", "Float"=>"float"}
    ignored_fields = ['_type', "_keywords"]

    # maintain list of fields you either want renamed or if you want them skipped by all transforms include them here
    renamed_fields = {"_id" => "mongo_id"}

    # hash of habtm relations from your schema - make sure the key is alpha before the value
    habtm = {'AlphaTable' => 'BetaTable'}

    relations_in = model.relations.select {|key,value| value[:relation]==Mongoid::Relations::Embedded::Many}
    single_in = model.relations.select {|key,value| value[:relation]==Mongoid::Relations::Embedded::One}
    models.each_with_index do |obj, i|
      id_to_use_next = @@id_counter[model_table] || 100000
      @@id_counter[model_table] = id_to_use_next+1
      obj_hash = {}
      obj_id = ""
      postgres_obj_id = ""
      model.fields.each do |field|
        next if ignored_fields.include?(field.first) or field.first.end_with?("_ids")
        field_name = field.first

        val = obj.send(field.first.to_sym)
        val = val.to_s if val.is_a?(BSON::ObjectId)
        # serialize hashes & arrays
        if val.is_a?(BSON::OrderedHash)
          val = val.to_h.to_yaml
        elsif val.is_a?(Array)
          val = val.map {|v| v.is_a?(BSON::OrderedHash) ? v.to_h : v }
          val = val.to_yaml
        elsif val.is_a?(Hash)
          val = val.to_yaml
        end

        # clean up any apostrophes
        val = val.gsub(/'/, "''") unless val.nil? || !val.is_a?(String)
        val = val.to_time.utc if val.present? && ([DateTime, Date, Time].include?(field[1].options[:type]))
        val = '' if val.nil? && (field[1].options[:type] == Time || field[1].options[:type] == DateTime || field[1].options[:type] == Date)

        if renamed_fields.include?(field_name)
          field_name = renamed_fields[field_name]
        elsif field_name=="number" # we were using a number field for user facing 'id'
          field_name = "id"
        elsif field_name.end_with?("_id")
          if val.blank?
            obj_hash[field_name] = ""
          else
            # throw in placeholder values with the bson id
            obj_hash[field_name] = "#{val}_placeholder"
            field_name = "mongo_#{field_name.to_s.gsub(/::/, "_").downcase}"
            @@id_indexed["#{val}_placeholder"] << @@lines.count
            @@all_objects["#{val}_placeholder"] = field_name
          end
        end
        obj_id = val if field_name=="mongo_id"
        postgres_obj_id = val if field_name=="id"
        obj_hash[field_name] = val
      end
      if poly_in
        obj_hash["mongo_#{poly_in}_id"] = parent_id
        obj_hash["#{poly_in}_type"] = parent_key.classify
        obj_hash["#{poly_in}_id"] = "#{parent_id}_placeholder"
        @@all_objects["#{parent_id}_placeholder"] = parent_key.classify
        @@id_indexed["#{parent_id}_placeholder"] << @@lines.count
      elsif !parent_key.nil?
        obj_hash["mongo_#{parent_key.to_sym}_id"] = parent_id
        obj_hash["#{parent_key.to_sym}_id"] = "#{parent_id}_placeholder"
        @@all_objects["#{parent_id}_placeholder"] = parent_key.classify
        @@id_indexed["#{parent_id}_placeholder"] << @@lines.count
      end
      if postgres_obj_id.blank?
        postgres_obj_id = id_to_use_next
        obj_hash["id"] = postgres_obj_id
      end
      @@id_hash[("#{obj_id}_placeholder")] = postgres_obj_id.to_s

      # create sql from array of hashes
      insert_sql(insert_string, obj_hash)

      # HABTM
      target=habtm[model.to_s]
      if target
        habtm_collection = obj.send("#{target.downcase}_ids")
        if habtm.size>0
          habtm_obj_hash = Hash.new
          habtm_collection.each do |target_obj|
            habtm_obj_hash["mongo_#{target.downcase}_id"] = target_obj.to_s
            habtm_obj_hash["#{target.downcase}_id"] = "#{target_obj.to_s}_placeholder"
            habtm_obj_hash["mongo_#{model.to_s.downcase}_id"] = obj._id
            habtm_obj_hash["#{model.to_s.downcase}_id"] = postgres_obj_id
            @@id_indexed["#{target_obj.to_s}_placeholder"] << @@lines.count
            insert_sql("INSERT INTO #{model_table}_#{target.to_s.gsub(/::/, "_").tableize} (", habtm_obj_hash)
          end
        end
      end
      single_in.each do |key, embedded|
        poly_as = embedded[:as] unless embedded[:as].blank?
        embed_collection = obj.send(key.to_sym)
        next if embed_collection.blank?
        generate_from_collection(embed_collection.class, [embed_collection], embedded[:inverse_class_name].to_s.gsub(/::/, "_").downcase, obj_id, poly_as) << "\n\r" unless embed_collection.blank?
      end
      relations_in.each do |key, embedded|
        poly_as = embedded[:as] unless embedded[:as].blank?
        embed_collection = obj.send(key.to_sym)
        generate_from_collection(embed_collection.first.class, embed_collection, embedded[:inverse_class_name].to_s.gsub(/::/, "_").downcase, obj_id, poly_as) << "\n\r" unless embed_collection.blank?
      end
    end
  end

  def insert_sql(insert_string, obj_hash)
    values = "'#{obj_hash.values.join("','")}'"
    values.gsub!(/'',/, "NULL,")
    values.gsub!(/,''/, ",NULL")
    new_sql_line = ""
    new_sql_line << insert_string
    new_sql_line << obj_hash.keys.join(",")
    new_sql_line << ") VALUES ("
    new_sql_line << values
    new_sql_line << "); "
    @@lines << new_sql_line
    new_sql_line
  end
end