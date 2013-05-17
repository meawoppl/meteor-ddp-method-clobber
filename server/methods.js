
c = new Meteor.Collection("testing");
c.insert({derp:"derp"});

Meteor.methods({
    working1:function(arrrrrrg) {
	this.setUserId(arrrrrrg);
	// return Arrrrrrg + "working";
    },

    working2:function(arrrrrrg) {
	// this.setUserId(arrrrrrg);
	return arrrrrrg + "working";
    },

    broken:function(arrrrrrg) {
	var res = c.findOne({})["_id"];
	this.setUserId(res);
	return res + "broken!";
    }
});