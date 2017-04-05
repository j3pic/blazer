

module Blazer
  module Adapters
    class DruidAdapter < BaseAdapter
      def initialize(data_source)
        @url=data_source.settings["url"]
      end
      def query_druid(hash)
        HTTParty.post "#{@url}/druid/v2/?pretty", body: hash.to_json, headers: {"Content-Type" => "application/json"}
      end
      def run_statement(statement, comment)
        raw_result = query_druid(JSON.parse statement)
        if raw_result.is_a?(Hash)
          [[], [], raw_result["error"] + ": " + raw_result["errorMessage"] + " (error of type " + raw_result["errorClass"] + ")"]
        else
          columns=raw_result[0].keys
          [columns, raw_result.map { |row|
             row.values
           }, nil]
        end
      end
      def adapter_name
        "DruidAdapter"
      end
    end
  end
end

