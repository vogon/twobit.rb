require 'net/http'
require 'neography'
require 'json'

# neo4j configuration
Neography.configure do |config|
end

@neo = Neography::Rest.new

work_queue = 
	["0000000000000000b6359c198b89747d41ebd833fc118e0d920a44ad63af0578"]

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
	# try and find existing node
	results = @neo.in_transaction(neo_tx,
		['match (n:Txn { hash: {hash} }) return id(n)', 
			{:hash => tx[:hash]}])["results"][0]

	if (results["data"].length > 0) then
		# node already exists
		return results["data"][0][0]
	else
		return @neo.in_transaction(neo_tx,
			["create (n:Txn {props}) return id(n)",
				{:props => tx}])
	end
end

while work_queue.length > 0 do
	# get next block
	block_id = work_queue.shift
	block_json = 
		Net::HTTP.get(URI("http://blockchain.info/rawblock/#{block_id}"))
	block = JSON.parse(block_json)

	# extract graph topology of block
	addresses = []
	txns = []
	input_edges = []
	output_edges = []

	block["tx"].each do |block_tx|
		txns << { hash: block_tx["hash"] }

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
end
