require 'spec_helper'

RSpec.describe PostgresToRedshift::Table do
  context 'with a simple table' do
    let(:dist_keys) { {} }
    let(:sort_keys) { {} }
    let(:attributes) do
      {
        "table_catalog" => "postgres_to_redshift",
        "table_schema" => "public",
        "table_name" => "films",
        "table_type" => "BASE TABLE",
      }
    end
    let(:columns) do
      [
        {
          "table_catalog"            => "postgres_to_redshift",
          "table_schema"             => "public",
          "table_name"               => "films",
          "column_name"              => "description",
          "ordinal_position"         => "2",
          "column_default"           => nil,
          "is_nullable"              => "YES",
          "data_type"                => "character varying",
          "character_maximum_length" => "255",
          "character_octet_length"   => "1020"
        }
      ]
    end


    let(:table) do
      described_class.new(attributes: attributes, columns: columns, dist_keys: dist_keys, sort_keys: sort_keys)
    end

    describe '#name' do
      it 'returns the name of the table' do
        expect(table.name).to eq("films")
      end
    end

    describe '#columns' do
      it 'returns a list of columns' do
        expect(table.columns.size).to eq(1)
        expect(table.columns.first.name).to eq("description")
      end
    end

    context 'with dist and sort keys specified' do
      let(:dist_keys) { { 'films' => 'description' } }
      let(:sort_keys) { { 'films' => ['description', 'other'] } }

      describe '#dist_key_for_create' do
        it 'returns the dist key' do
          expect(table.dist_key_for_create).to eq(' DISTSTYLE KEY DISTKEY (description)')
        end
      end

      describe '#sort_keys_for_create' do
        it 'only returns valid columns' do
          expect(table.sort_keys_for_create).to eq(' SORTKEY (description)')
        end
      end
    end
  end

  describe '#is_view?' do
    it 'returns true if it is a view' do
      attributes = {
        "table_catalog" => "postgres_to_redshift",
        "table_schema" => "public",
        "table_name" => "films",
        "table_type" => "VIEW",
      }

      table = PostgresToRedshift::Table.new(attributes: attributes)
      expect(table.is_view?).to be_truthy
    end

    it 'returns false if it is not a view' do
      attributes = {
        "table_catalog" => "postgres_to_redshift",
        "table_schema" => "public",
        "table_name" => "films",
        "table_type" => "BASE TABLE",
      }

      table = PostgresToRedshift::Table.new(attributes: attributes)
      expect(table.is_view?).to be_falsey
    end
  end

  describe 'target_table_name' do
    it 'strips _view from the end of the table name' do
      attributes = {
        "table_catalog" => "postgres_to_redshift",
        "table_schema" => "public",
        "table_name" => "films_view",
        "table_type" => "VIEW",
      }

      table = PostgresToRedshift::Table.new(attributes: attributes)
      expect(table.target_table_name).to eq("films")
    end
  end
end
