module Schlepp
  module Source
    class Sequel
      def initialize(sequel, model)
        @sequel = sequel
        @model = model
      end

      def columns
        @model.columns
      end

      def each
        @sequel.each do |row|
          stringified_row = row.values.map { |v| v.to_s }

          yield [stringified_row]
        end
      end
    end
  end
end
