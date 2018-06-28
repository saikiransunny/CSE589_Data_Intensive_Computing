function wordcloud()
{
var name=document.getElementById("del").value;
if(name==="h1b"){
	var fill = d3.scale.category20();

var cityData = [], 
    width = 500, 
    height = 500;

d3.tsv("bloody.tsv", function(data)
{
    // build the list of city names
    data.forEach( function (d) {
        cityData.push(d);
    });

    d3.layout.cloud().size([800, 300])
        .words(cityData.map(function(d) {
            return {text: d.words, size:+d.count/3};
        }))
        .rotate(function() { return ~~(Math.random() * 2) * 30; })
        .font("Impact")
        .fontSize(function(d) { return d.size; })
        .on("end", draw)
        .start();

});

function draw(words) {	
d3.select("body").append("svg")
    .attr("width", 850)
    .attr("height", 350)
    .append("g")
    .attr("transform", "translate(320,200)")
    .selectAll("text")
    .data(words)
    .enter().append("text")
    .style("font-size", function(d) { return d.size + "px"; })
    .style("font-family", "Impact")
    .style("fill", function(d, i) { return fill(i); })
    .attr("text-anchor", "middle")
    .attr("transform", function(d) {
        return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
    })
    .text(function(d) { return d.text; });
}

}
else{
	var fill = d3.scale.category20();

var cityData = [], 
    width = 500, 
    height = 500;

d3.tsv("newyork_h1.tsv", function(data)
{
    // build the list of city names
    data.forEach( function (d) {
        cityData.push(d);
    });

    d3.layout.cloud().size([800, 300])
        .words(cityData.map(function(d) {
            return {text: d.words, size:+d.count/3};
        }))
        .rotate(function() { return ~~(Math.random() * 2) * 30; })
        .font("Impact")
        .fontSize(function(d) { return d.size; })
        .on("end", draw)
        .start();

});

function draw(words) {
d3.select("body").append("svg")
    .attr("width", 850)
    .attr("height", 350)
    .append("g")
    .attr("transform", "translate(320,200)")
    .selectAll("text")
    .data(words)
    .enter().append("text")
    .style("font-size", function(d) { return d.size + "px"; })
    .style("font-family", "Impact")
    .style("fill", function(d, i) { return fill(i); })
    .attr("text-anchor", "middle")
    .attr("transform", function(d) {
        return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
    })
    .text(function(d) { return d.text; });
}

}
}