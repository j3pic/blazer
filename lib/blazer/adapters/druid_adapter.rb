module Blazer
  module Adapters
    class DruidAdapter < BaseAdapter
      def match_extended_date(str)
        /\s*(now|'(\d+\.?\d*)\s+(\w+)'|'(\d+\.?\d*)\s+(\w+)\s+(ago|from now|hence|later|away|out)')\s*/i.match(str)
      end

      def match_iso_date(str)
        /[0-9]+-[0-9]{1,2}-[0-9]{1,2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.*/.match(str)
      end
      
      def parse_extended_date(str)
        m = match_extended_date(str)
        raise "Not a valid date: #{str}" unless m
        if m[1] == 'now'
          Time.now
        elsif m[2]
          m[2].to_f.send(m[3].to_sym)
        elsif m[6] == 'ago'
          Time.now - m[4].to_f.send(m[5].to_sym)
        elsif /(from now|hence|later|away|out)/.match(m[6])
          Time.now + m[4].to_f.send(m[5].to_sym)
        else
          raise "Not a valid date: #{str}"
        end
      end

      def parse_date(str)
        if match_iso_date(str)
          str.to_date
        else
          begin
            parse_extended_date("'" + str + "'")
          rescue RuntimeError
            parse_extended_date(str)
          end
        end
      end
      
      def combine_dates(dates, operators)
        operators = operators.reverse
        dates.reduce { |sum, date| sum.send(operators.pop, date)}
      end

      def parse_date_expr(str)
        if match_extended_date(str)
          dates = str.split(/[\+-]/)
          operators = str.scan(/[\+-]/)
          combine_dates(dates.map { |d| parse_date(d) }, operators.map(&:first).map(&:to_sym))
        else
          parse_date(str)
        end
      end

      def date_interval(str)
        from, to = str.split('/')
        raise 'Need two dates separated by `/`' unless from && to
        [parse_date_expr(from), parse_date_expr(to)].map { |d| d.in_time_zone('Zulu') }
      end
      
      def initialize(data_source)
        @url=data_source.settings["url"]
        @extended_operators = [:select]
      end
      
      def with_symbol_keys(hash)
        result = {}
        hash.keys.each do |k|
          result[k.to_sym] = hash[k]
        end
        result
      end
      
      def query_druid(hash)
        result = HTTParty.post "#{@url}/druid/v2/?pretty", body: no_extended_operators(hash).to_json, headers: {"Content-Type" => "application/json"}
        result
      end
      
      def render_timeseries(orig_query,data)
        # [columns, values, error=nil]
        [["timestamp"] + data[0]["result"].keys,
         data.map do |row|
           [row["timestamp"]] + row["result"].values
         end, nil]
      end
      
      def flatten_gently(array)
        result = []
        array.each do |elem|
          if elem.is_a?(Array)
            result += elem
          else
            result.push elem
          end
        end
        result
      end
      
      def render_topN(orig_query,data)
        [["timestamp"] + data[0]["result"][0].keys,
         flatten_gently(data.map do |timestamp_row|
                          timestamp_row["result"].map do |inner_row|
                            [timestamp_row["timestamp"]] + inner_row.values
                          end
                        end),nil]
      end
    
      def extended_operators(hash)
        hash.slice(*@extended_operators)
      end

      def no_extended_operators(hash)
        hash.except(*@extended_operators)
      end

      def run_statement(statement, comment)
        parsed_statement = with_symbol_keys(JSON.parse statement)
        if parsed_statement[:intervals]
          parsed_statement[:intervals] = parsed_statement[:intervals].map do |interval_string|
            date_interval(interval_string).map do |date|
              date.iso8601
            end.join("/")
          end
        end
        raw_result = query_druid(parsed_statement)
        if raw_result.is_a?(Hash)
          [[], [], raw_result["error"] + ": " + raw_result["errorMessage"] + " (error of type " + raw_result["errorClass"] + ")"]
        elsif raw_result.length == 0
          [[],[],nil]
        elsif parsed_statement[:queryType] == "timeseries" 
          render_timeseries(parsed_statement, raw_result)
        elsif parsed_statement[:queryType] == "topN"
          render_topN(parsed_statement,raw_result)
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


