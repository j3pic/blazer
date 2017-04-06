module Blazer
  module Adapters
    class DruidAdapter < BaseAdapter
      def match_extended_date(str)
        /\s*(now|'(\d+\.?\d*)\s+(\w+)'|'(\d+\.?\d*)\s+(\w+)\s+(ago|from now|hence|later|away|out)')\s*/i.match(str)
      end

      def match_iso_date(str)
        str.is_a?(String) && /[0-9]+-[0-9]{1,2}-[0-9]{1,2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.*/.match(str)
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
        @extended_operators = [:select,:raw,:orderBy,:orderByDirection]
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
          
      def extended_operators(hash)
        hash.slice(*@extended_operators)
      end

      def no_extended_operators(hash)
        hash.except(*@extended_operators)
      end

      def select_invalid_columns?(selected_columns, columns)
        selected_columns.each { |col|
          if ([col] & columns).length == 0
            return col
          end
        }
        false
      end
      
      def select(selected_columns, columns, rows, error)
        invalid = select_invalid_columns?(selected_columns,columns)
        if invalid
          [[],[],"Invalid column selected: #{invalid}"]
        else
          [selected_columns, rows.map do |row|
             result=[]
             selected_columns.map do |colname|
               ix = columns.index colname
               result.push(row[ix])
             end
             result
           end,nil]
        end
      end

      def order_by(sort_by_columns, ascendingp, columns, rows, error)
        invalid = select_invalid_columns?(sort_by_columns, columns)
        indices = sort_by_columns.map do |col|
          columns.index(col)
        end
        basic_cmp = if ascendingp
                      lambda { |a,b| a<b }
                    else
                      lambda { |a,b| a>b }
                    end
        cmp = lambda { |a,b|
          if match_iso_date(a)
            a=a.to_datetime
            b=b.to_datetime
          end
          basic_cmp.call(a,b)
        } 
        if invalid
          [[],[],"Invalid orderBy column: #{invalid}"]
        else
          sorted_rows = rows.sort do |row_a,row_b|
            catch :verdict do
              indices.each do |ix|
                if cmp.call(row_a[ix],row_b[ix])
                  throw :verdict, -1
                elsif cmp.call(row_b[ix],row_a[ix])
                  throw :verdict, 1
                end
              end
              throw :verdict, 0
            end
          end
          [columns,sorted_rows,error]
        end
      end

      def postprocess_result(raw_result,parsed_statement)
        if raw_result.is_a?(Hash)
          [[], [], raw_result["error"] + ": " + raw_result["errorMessage"] + " (error of type " + raw_result["errorClass"] + ")"]
        elsif raw_result.length == 0
          [[],[],nil]
        elsif parsed_statement[:raw]
          [["raw_result"],[[raw_result.to_json]],nil]
        else
          flattened = flatten_structure(raw_result).uniq
          columns=flattened[0].keys
          result=[columns, flattened.map { |row|
                    row.values
                  }, nil]
          if parsed_statement[:select]
            result=select(parsed_statement[:select], *result)
          end
          if parsed_statement[:orderBy]
            ascendingp = case parsed_statement[:orderByDirection]
                         when "ascending"
                           true
                         when nil
                           true
                         when "descending"
                           false
                         else
                           return [[],[],"Invalid orderByDirection: #{parsed_statement[:orderByDirection]}. Valid values are \"ascending\" or \"descending\""]
                         end
            result=order_by(parsed_statement[:orderBy],ascendingp,*result)
          end
          result
        end
      end
      
      def run_statement(statement, comment)
        parsed_statement = begin
                             with_symbol_keys(JSON.parse statement)
                           rescue JSON::ParserError => e
                             return [[],[],e.message]
                           end
        if parsed_statement[:intervals]
          parsed_statement[:intervals] = parsed_statement[:intervals].map do |interval_string|
            date_interval(interval_string).map do |date|
              date.iso8601
            end.join("/")
          end
        end
        postprocess_result(query_druid(parsed_statement),parsed_statement)
      end

      def join_hashes(first_hash,other_hash, supersede: false)
        result=first_hash.except
        other_hash.each { |k,v|
          if (!result[k]) || supersede
            result[k] = v
          end
        }
        result
      end

      def join_hash_to_array_of_hashes(hash, array)
        array.map do |hash_n|
          join_hashes hash, hash_n
        end
      end

      def join_hash_to_flat_array(hash, k, array)
        array.map do |n|
          next_hash = hash.except
          next_hash[k] = n
          next_hash
        end
      end

      def join_hash_array_to_hash_array(hasharr_a, hasharr_b)
        flatten_gently(hasharr_a.map do |hash|
                         join_hash_to_array_of_hashes(hash,hasharr_b)
                       end)
      end

      def join_hash_array_to_flat_array(hasharr, flat_key, flatarr)
        flatten_gently(hasharr.map do |hash|
                         join_hash_to_flat_array hash, flat_key, flatarr
                       end)
      end

      def join_to_flat_array(hash_or_hasharr, key, flatarr)
        if hash_or_hasharr.is_a?(Hash)
          join_hash_to_flat_array(hash_or_hasharr, key, flatarr)
        else
          join_hash_array_to_flat_array(hash_or_hasharr, key, flatarr)
        end
      end

      def join_to_hash_array(hash_or_hasharr, hash_arr)
        if hash_or_hasharr.is_a?(Hash)
          join_hash_to_array_of_hashes(hash_or_hasharr, hash_arr)
        else
          join_hash_array_to_hash_array(hash_or_hasharr, hash_arr)
        end
      end

      def join_to_hash(hash_or_hasharr, hash)
        if hash_or_hasharr.is_a?(Hash)
          join_hashes hash_or_hasharr, hash
        else
          join_to_hash_array(hash_or_hasharr, hash)
        end
      end

      def flat?(obj)
        if obj.is_a?(Hash)
          obj.each do |k,v|
            if v.is_a?(Hash) || v.is_a?(Array)
              return false
            end
          end
          return true
        elsif obj.is_a?(Array)
          obj.each do |elem|
            if !elem.is_a?(Hash)
              return false
            end
          end
          return true
        else
          true
        end
      end
      
      def flatten_hash_structure_1(hash)
        result={}
        hashes=[]
        flat_arrays={}
        hash_arrays=[]
        hash.each do |k,v|
          v = flatten_structure(v)
          if v.is_a?(Hash)
            hashes.push(v)
          elsif v.is_a?(Array)
            v = flatten_structure(v.flatten)
            if v.length == 0 || !(v[0].is_a?(Hash))
              flat_arrays[k]=v
            else
              hash_arrays.push(v.map do |elem|
                                 flatten_structure(elem)
                               end)
            end
          else
            result[k] = v
          end
        end
        
        flat_arrays.each do |ek,ev|
          result = join_to_flat_array(result, ek, ev)
        end
        hash_arrays.each do |elem|
          result = join_to_hash_array(result,elem)
        end
        hashes.each do |elem|
          result = join_to_hash(result, elem)
        end
        result
      end

      def flatten_hash_structure(hash)
        result=hash
        while(!flat?(result))
          result = flatten_hash_structure_1(result)
        end
        result
      end

      def flatten_hash_array(hasharr)
        result=[]
        hasharr.each do |hash|
          new_hash = flatten_hash_structure(hash)
          if new_hash.is_a?(Array)
            result += new_hash
          else
            result.push(new_hash)
          end
        end
        result
      end

      def flatten_structure(s)
        if s.is_a?(Hash)
          flatten_hash_structure(s)
        elsif s.is_a?(Array)
          flatten_hash_array(s)
        else
          s
        end
      end

      
      def adapter_name
        "DruidAdapter"
      end
    end
  end
end


