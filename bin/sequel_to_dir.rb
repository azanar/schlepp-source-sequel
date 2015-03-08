#!/usr/bin/env ruby

$LOAD_PATH.push(File.join(File.expand_path(File.dirname(__FILE__)) , '../lib'))

require 'hydrogen'
require 'schlepp'
require 'schlepp/sink/filter/chunker'
require 'schlepp/sink/filter/compressor'
require 'schlepp/sink/filter/formatter/csv'

require 'sequel'
require 'schlepp/source/sequel'

require 'uri'
require 'pp'

require 'schlepp-sink-fs'

config = {
  :table_name => 'foo',
  :columns => %w{id first second}
}

model = Hydrogen::Model.new(config)

db = Sequel.postgres(:database => "schlepp_source")

dataset = dataset = db["SELECT * FROM data"]

source = Schlepp::Source::Sequel.new(dataset, model)

sink = Schlepp::Sink::Fs.new(model, URI('file:///tmp/foo/'),
                             :filters => [Schlepp::Sink::Filter::Formatter::Csv.new,
                                          Schlepp::Sink::Filter::Chunker.new(:chunk_size => 40000),
                                          Schlepp::Sink::Filter::Compressor.new])

res = Schlepp.schlepp(source, sink)


res.each do |r|
  puts r.url.path
end
