require 'date'
require 'json'
require 'net/http'
require 'neography'

# neo4j configuration
Neography.configure do |config|
	config.log_enabled = true
end

@neo = Neography::Rest.new

work_queue = 
	["00000000000000006d5bc2f2504a85075b78f8f4c1515ced842629ade7c201b3"]

def get_or_create_address_node(neo_tx, address)
	# try and find existing node
	results = @neo.in_transaction(neo_tx,
		['match (n:Address { addr: {addr} }) return id(n)', 
			{:addr => address[:addr]}])["results"][0]

	if (results["data"].length > 0) then
		# node already exists
		return results["data"][0][0]
	else
		return @neo.in_transaction(neo_tx,
			["create (n:Address {props}) return id(n)",
				{:props => address}])
	end
end

def get_or_create_tx_node(neo_tx, tx)
	# puts "get_or_create_tx_node(#{tx[:hash]})"

	# # try and find existing node
	# results = @neo.in_transaction(neo_tx,
	# 	['match (n:Txn { hash: {hash} }) return id(n)', 
	# 		{:hash => tx[:hash]}])["results"][0]

	# if (results["data"].length > 0) then
	# 	# node already exists
	# 	return results["data"][0][0]
	# else
		return @neo.in_transaction(neo_tx,
			["create (n:Txn {props}) return id(n)",
				{:props => tx}])
	# end
end

while work_queue.length > 0 do
	# get next block
	block_id = work_queue.shift
	print "working on block #{block_id}... "

	block_json = 
		Net::HTTP.get(URI("http://blockchain.info/rawblock/#{block_id}"))
	block = JSON.parse(block_json)

	puts "height #{block["height"]}"

	# extract graph topology of block
	addresses = []
	txns = []
	input_edges = []
	output_edges = []

	block["tx"].each do |block_tx|
		txns << { hash: block_tx["hash"], time: block_tx["time"] }

		block_tx["inputs"].each do |input|
			# 0-input txns have inputs: [{}], for some reason
			next if input == {}

			addresses << { addr: input["prev_out"]["addr"] }
			input_edges << 
				{ 
					addr: input["prev_out"]["addr"],
					tx: block_tx["hash"],
					value: input["prev_out"]["value"]
				}
		end

		block_tx["out"].each do |output|
			addresses << { addr: output["addr"] }
			output_edges <<
				{
					addr: output["addr"],
					tx: block_tx["hash"],
					value: output["value"]
				}
		end
	end

	neo_tx = @neo.begin_transaction

	# add nodes for addresses and transactions in block
	addresses.each do |addr|
		# puts "adding address node for " + addr.inspect
		get_or_create_address_node(neo_tx, addr)
	end

	# HACK: since we reuse existing address nodes anyway, commit address nodes
	# in a separate transaction
	@neo.commit_transaction(neo_tx)

	neo_tx = @neo.begin_transaction

	txns.each do |tx|
		# puts "adding txn node for " + tx.inspect
		get_or_create_tx_node(neo_tx, tx)
	end

	# draw edges for inputs and outputs
	input_edges.each do |input|
		# puts "drawing edge for input " + input.inspect
		@neo.in_transaction(neo_tx,
			["match (m:Address {addr: {addr}})," +
			 	   "(n:Txn {hash: {tx}})" +
			 "create unique (m)-[:SENT {props}]->(n)",
			 {
			 	:addr => input[:addr],
			 	:tx => input[:tx],
			 	:props => {
			 		value: input[:value]
			 	}
			 }])
	end

	output_edges.each do |input|
		# puts "drawing edge for output " + input.inspect
		@neo.in_transaction(neo_tx,
			["match (m:Address {addr: {addr}})," +
			 	   "(n:Txn {hash: {tx}})" +
			 "create unique (n)-[:SENT {props}]->(m)",
			 {
			 	:addr => input[:addr],
			 	:tx => input[:tx],
			 	:props => {
			 		value: input[:value]
			 	}
			 }])
	end

	@neo.commit_transaction(neo_tx)

	# add newly-discovered blocks
	# TODO: add some way of discovering blocks "down" the blockchain
	if (block["prev_block"] != "") then
		# temporary: if this block is more than a day old, stop
		break if (DateTime.now - DateTime.strptime(block["time"].to_s, '%s') >= 86400)

		work_queue << block["prev_block"]
	end
end
