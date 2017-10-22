

// Duplicate Removal
db.reviews.aggregate([{$group:{_id:"$url", dups:{$push:"$_id"}, count: {$sum: 1}}},
{$match:{count: {$gt: 1}}}
]).forEach(function(doc){
  doc.dups.shift();
  db.reviews.remove({_id : {$in: doc.dups}});
});

db.reviews.find(
	{rating : {$exists : true}}).forEach( 
		function(obj) { 
			obj.rating = new NumberInt( obj.rating ); 
			db.reviews.save(obj); 
			} );

db.reviews.aggregate(	
	{$group:{_id:"$company",avgRating:{$avg:"$rating"}}},
	{$sort:{avgRating:-1}}
);

	
db.reviews.aggregate(	
	{$group:{_id:"$company", count:{$sum:1}}},
	{$match:{"count":{$lt:100}}},
	{$sort:{count:-1}}	
);

db.reviews.aggregate(	{$group:{_id:"$company", count:{$sum:1}}},{$match:{"count":{$lt:100}}},{$sort:{count:-1}}	);