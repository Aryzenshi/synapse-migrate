// Idempotently load the bundled Extended JSON fixture into the local demo replica set.
const fixture = EJSON.parse(cat("/fixtures/mongo_sample.json"));
const demo = db.getSiblingDB("shop");
const customers = fixture.filter((document) => Object.prototype.hasOwnProperty.call(document, "email"));
const tags = fixture.filter((document) => Object.prototype.hasOwnProperty.call(document, "name"));

if (customers.length && demo.customers.countDocuments() === 0) {
  demo.customers.insertMany(customers);
}
if (tags.length && demo.tags.countDocuments() === 0) {
  demo.tags.insertMany(tags);
}
printjson({
  database: "shop",
  customers: demo.customers.countDocuments(),
  tags: demo.tags.countDocuments(),
});
