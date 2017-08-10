const express = require('express');
const csv = require('csv');
const fs = require('fs');
const path = require('path');


const parseEntriesWithFilter = (filename, filterFn) => (new Promise(
  (resolve, reject) => {
    const output = [];

    const parser = csv.parse({columns: true});
    parser.on('readable', () => {
      try {
        while(data = parser.read()){
          cleanInts(data)
          filterFn(data) && output.push(data);
        }
      } catch (err) {
        reject(err)
      }
    });

    parser.on('finish', () => resolve(output));
    parser.on('error', reject);

    fs.createReadStream(path.join(__dirname, 'db', filename)).pipe(parser);
  }
));

const cleanInts = obj => {
  for (const key in obj) {
    if (key === 'created_at' || key === 'updated_at' || key === 'international_departure_date') {
      obj[key] = dateMaker(obj[key]);
    } else {
      obj[key] = parseInt(obj[key]) || obj[key];
    }
  }
}

const dateMaker = datestr => {
  const from = datestr.split(' ')[0].split("-");
  return new Date(from[0], from[1] - 1, from[2]);
}


const app = express();

/* SINGLE ROUTE */
app.get('/api/v1/shipments', async (req, res) => {
  if (!req.query.company_id) {
    res.status(422).json({ errors: ['company_id is required'] });
    return;
  }

  try {
    const shipments = await parseEntriesWithFilter('shipments.csv', shipment => shipment.company_id === parseInt(req.query.company_id));

    let records = await Promise.all(
      shipments.map(async shipment => {
        const shiprods = await parseEntriesWithFilter('shipment_products.csv', shiprod => shiprod.shipment_id === shipment.id);

        const products = await Promise.all(
          shiprods.map(async shiprod =>
            Object.assign(
              (await parseEntriesWithFilter('products.csv', product => product.id === shiprod.product_id))[0],
              { quantity: shiprod.quantity },
              {
                active_shipment_count: (
                  await parseEntriesWithFilter('shipment_products.csv', shiprod2 => shiprod2.product_id === shiprod.product_id)
                ).length
              }
            )
          )
        );

        return Object.assign({}, shipment, { products });
      })
    );

    // Sort / Filter / Paginate records
    if (req.query.sort) {
      records = records.sort((a,b) => (req.query.direction === 'desc' ? -1 : 1) * (a[req.query.sort] < b[req.query.sort] ? -1 : 1));
    }

    if (req.query.international_transportation_mode) {
      records = records.filter(record => record.international_transportation_mode === req.query.international_transportation_mode);
    }

    const page = parseInt(req.query.page || 1);
    const per = parseInt(req.query.per || 4);
    records = records.slice((page-1)*per, page*per);

    res.json({ records });
  } catch (err) {
    res.status(422).json({ errors: [err.message] });
  }
});


/* RUN SERVER ON PORT 3000 */
app.listen(3000, () => {
  console.log("Hexport Server running on port 3000");
});
