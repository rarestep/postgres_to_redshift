# table_catalog                | postgres_to_redshift
# table_schema                 | public
# table_name                   | acquisition_pages
# table_type                   | BASE TABLE
# self_referencing_column_name |
# reference_generation         |
# user_defined_type_catalog    |
# user_defined_type_schema     |
# user_defined_type_name       |
# is_insertable_into           | YES
# is_typed                     | NO
# commit_action                |
#
class PostgresToRedshift
  class Table
    attr_accessor :attributes, :columns

    def initialize(attributes: , columns: [], dist_keys: {}, sort_keys: {})
      self.attributes = attributes
      self.columns = columns
      @dist_key = dist_keys.fetch(name, nil)
      @sort_keys = sort_keys.fetch(name, [])
    end

    def name
      attributes["table_name"]
    end
    alias_method :to_s, :name

    def target_table_name
      name.gsub(/_view$/, '')
    end

    def columns=(column_definitions = [])
      @columns = column_definitions.map do |column_definition|
        Column.new(attributes: column_definition)
      end
    end

    def columns_for_create
      columns.map do |column|
        %Q["#{column.name}" #{column.data_type_for_copy}]
      end.join(", ")
    end

    def dist_key_for_create
      return '' unless @dist_key
      return '' unless columns.map(&:name).include?(@dist_key)
      " DISTSTYLE KEY DISTKEY (#{@dist_key})"
    end

    def sort_keys_for_create
      valid_sort_keys = @sort_keys.select { |column| columns.map(&:name).include?(column) }
      return '' unless valid_sort_keys.any?
      " SORTKEY (#{valid_sort_keys.join(', ')})"
    end

    def columns_for_copy
      columns.map do |column|
        column.name_for_copy
      end.join(", ")
    end

    def is_view?
      attributes["table_type"] == "VIEW"
    end
  end
end
