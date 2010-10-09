require 'active_record'
require 'active_support'
require 'activerecord-fast-import'

ActiveRecord::Base.establish_connection(YAML::load(File.open('database.yml')))

#PartySponsor.fast_import('sponzori_stran.csv', :fields_terminated_by => ',' , :fields_optionally_enclosed_by => '"')

def create_mysql_table name, columns
  sql = "CREATE TABLE IF NOT EXISTS `#{name.strip}` ("

  for column in columns.split(',')
    sql << "`#{column.strip}` VARCHAR(500),"
  end

  sql.gsub!(/,$/, '')
  sql << ')'

  ActiveRecord::Base.connection.execute(sql)
end

def import_data name, csv
  class_name = ActiveSupport::Inflector.classify(name)

  eval <<-EOF
    class #{class_name} < ActiveRecord::Base
      set_table_name '#{name}'
    end
  EOF

  class_name.constantize.fast_import(csv, :fields_terminated_by => ',' , :fields_optionally_enclosed_by => '"', :ignore_lines => 1)

end

def table_exists? name
  sql = "SELECT COUNT(*) AS table_exists FROM information_schema.tables WHERE table_schema = 'datanest' AND table_name = '#{name}'"
  result = ActiveRecord::Base.connection.execute(sql)
  row = result.fetch_row
  row.size > 0 and row[0] == 1
end

def import_from_file csv
  name = File.basename(csv)[0..-10]
  columns = ''

  File.open(csv, 'r') do |f|
    columns = f.readline
  end

  unless table_exists?(name)
    create_mysql_table name, columns
    import_data name, csv
  end
end

Dir['csv/*.csv'].each do |f|
  import_from_file f
end
