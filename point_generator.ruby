#!/usr/bin/ruby

i = 0

while (i < ARGV[0].to_i) do
  puts "#{Random.rand(0.0...100.0).round(2)},#{Random.rand(0.00...100.00).round(2)}"

  i += 1
end
