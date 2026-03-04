use('mpcs53001_final_project');

// 1. Retrieve all products in the "fashion" category along with their associated attributes such as size, color, and material.
db.product_specs.find(
  {
    category: "Fashion",
    $or: [
      { "attributes.color": "Blue" },
      { "attributes.size":  "L" }
    ]
  },
  {
    _id: 0,
    sku: 1,
    category: 1,
    "attributes.color": 1,
    "attributes.size": 1,
    "attributes.material": 1
  }
);

// 2. Retrieve all products in the "fashion" category that are available in either blue color or large size.
db.product_specs.find(
  { category: "Fashion" },
  {
    _id: 0,
    sku: 1,
    category: 1,
    attributes: 1
  }
)
;