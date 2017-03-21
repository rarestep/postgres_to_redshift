require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'zlib'
require 'stringio'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :target_uri, :target_schema, :target_tables
    attr_writer :dist_keys, :sort_keys

    def dist_keys
      @dist_keys ||= {}
    end

    def sort_keys
      @sort_keys ||= {}
    end
  end

  attr_reader :source_connection, :target_connection, :s3

  def self.reset_connections
    @source_connection.close if @source_connection
    @target_connection.close if @target_connection

    @source_connection = nil
    @target_connection = nil
  end

  def self.update_tables
    update_tables = PostgresToRedshift.new

    update_tables.tables.each do |table|
      target_connection.exec <<-SQL
        CREATE TABLE IF NOT EXISTS #{self.target_schema}.#{table.target_table_name}
        (#{table.columns_for_create})
        #{table.dist_key_for_create}
        #{table.sort_keys_for_create}
      SQL

      update_tables.copy_table(table)

      update_tables.import_table(table)
    end
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
  end

  def self.target_schema
    @target_schema ||= 'public'
  end

  def self.source_connection
    if @source_connection.nil?
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def self.target_connection
    if @target_connection.nil?
      @target_connection = PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end

    @target_connection
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def tables
    tables = source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW')").
      map { |table_attributes| Table.new(attributes: table_attributes, dist_keys: self.class.dist_keys, sort_keys: self.class.sort_keys) }.
      reject { |table| table.name =~ /^pg_/ }

    tables = tables.select { |table| self.class.target_tables.include?(table.name) } if self.class.target_tables

    tables.each { |table| table.columns = column_definitions(table) }
    tables
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['S3_DATABASE_EXPORT_ID'], secret_access_key: ENV['S3_DATABASE_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['S3_DATABASE_EXPORT_BUCKET']]
  end

  def copy_table(table)
    buffer = StringIO.new
    zip = Zlib::GzipWriter.new(buffer)

    puts "Downloading #{table}"
    copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name}) TO STDOUT WITH CSV DELIMITER '|'"

    source_connection.copy_data(copy_command) do
      while row = source_connection.get_copy_data
        zip.write(row)
      end
    end
    zip.finish
    buffer.rewind
    upload_table(table, buffer)
  end

  def upload_table(table, buffer)
    puts "Uploading #{table.target_table_name}"
    bucket.objects["export/#{table.target_table_name}.psv.gz"].delete
    bucket.objects["export/#{table.target_table_name}.psv.gz"].write(buffer, acl: :authenticated_read)
  end

  def import_table(table)
    puts "Importing #{table.target_table_name}"
    target_connection.exec("DROP TABLE IF EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}_updating")

    target_connection.exec("BEGIN;")

    target_connection.exec("ALTER TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name} RENAME TO #{table.target_table_name}_updating")

    target_connection.exec("CREATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name} (#{table.columns_for_create})")

    target_connection.exec("COPY #{PostgresToRedshift.target_schema}.#{table.target_table_name} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' CSV GZIP TRUNCATECOLUMNS DELIMITER as '|' ;")

    target_connection.exec("COMMIT;")
  end
end
