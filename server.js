import express from 'express'
import cors from 'cors'
import { MongoClient } from 'mongodb';
const uri = "mongodb+srv://db_user_read:LdmrVA5EDEv4z3Wr@cluster0.n10ox.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

const app = express();
const port = 4000;

app.use(express.json());
app.use(cors());
import moment from 'moment'

app.get('/',(req,res)=>{
    res.send("API working")
})

//total number of customers 
app.get('/api/customers', async (req,res)=>{

    try{
        const db = client.db('RQ_Analytics');
        const result = await db.collection('shopifyCustomers')
      .find()
      .toArray();
        res.send(result);
        console.log(result)
    } catch(error){
        console.error('Error fetching shopifyCustomers:', error);
    }
})

//total number of products
app.get('/api/products', async (req,res)=>{

    try{
        const db = client.db('RQ_Analytics');
        const result = await db.collection('shopifyProducts')
      .find()
      .toArray();
        res.send(result);
        console.log(result)
    } catch(error){
        console.error('Error fetching shopifyProducts:', error);
    }
})

//total number of orders
app.get('/api/totalorders', async (req,res)=>{

  try{
      const db = client.db('RQ_Analytics');
      const result = await db.collection('shopifyOrders')
    .find()
    .toArray();
      res.send(result);
      console.log(result)
  } catch(error){
      console.error('Error fetching shopifyOrders:', error);
  }
})


//total sales
app.get('/api/totalsales' ,async(req,res)=>{

    try{
        const db = client.db('RQ_Analytics');
        const dailySales = await db.collection('shopifyOrders')
        .aggregate([
          {
            $addFields: {
              createdAtDate: { $toDate: '$created_at' }
            }
          },
          {
            $group: {
              _id: {
                year: { $year: '$createdAtDate' },
                month: { $month: '$createdAtDate' },
                day: { $dayOfMonth: '$createdAtDate' }
              },
              totalPrice: { $sum: { $toDouble: '$total_price' } }
            }
          },
          { $sort: { '_id.year': 1, '_id.month': 1, '_id.day': 1 } }
        ])
        .toArray();

        const monthlySales = await db.collection('shopifyOrders')
      .aggregate([
        {
          $addFields: {
            createdAtDate: { $toDate: '$created_at' }
          }
        },
        {
          $group: {
            _id: {
              year: { $year: '$createdAtDate' },
              month: { $month: '$createdAtDate' }
            },
            totalPrice: { $sum: { $toDouble: '$total_price' } }
          }
        },
        { $sort: { '_id.year': 1, '_id.month': 1 } }
      ])
      .toArray();
  
      const quarterlySales = await db.collection('shopifyOrders')
      .aggregate([
        {
          $addFields: {
            createdAtDate: { $toDate: '$created_at' }
          }
        },
        {
          $group: {
            _id: {
              year: { $year: '$createdAtDate' },
              quarter: { $ceil: { $divide: [{ $month: '$createdAtDate' }, 3] } }
            },
            totalPrice: { $sum: { $toDouble: '$total_price' } }
          }
        },
        { $sort: { '_id.year': 1, '_id.quarter': 1 } }
      ])
      .toArray();
  
      const yearlySales = await db.collection('shopifyOrders')
      .aggregate([
        {
          $addFields: {
            createdAtDate: { $toDate: '$created_at' }
          }
        },
        {
          $group: {
            _id: { $year: '$createdAtDate' },
            totalPrice: { $sum: { $toDouble: '$total_price' } }
          }
        },
        { $sort: { '_id': 1 } }
      ])
      .toArray();

      res.json({ dailySales, monthlySales, quarterlySales, yearlySales })
    } catch(error){
        console.error('Error calculating total sales:', error)
    }  
})

//total yearly sales
app.get('/api/totalYearlySales' ,async (req,res)=>{
  try {
    const db = client.db('RQ_Analytics');
    const yearlySales = await db.collection('shopifyOrders')
    .aggregate([
      {
        $addFields: {
          createdAtDate: { $toDate: '$created_at' }
        }
      },
      {
        $group: {
          _id: { $year: '$createdAtDate' },
          totalPrice: { $sum: { $toDouble: '$total_price' } }
        }
      },
      { $sort: { '_id': 1 } }
    ])
    .toArray();
    res.json({yearlySales})
  } catch (error) {
    
  }
})

app.get('/api/customersadded' ,async(req,res)=>{

    try{
        const db = client.db('RQ_Analytics');
        const dailyAdded = await db.collection('shopifyCustomers')
        .aggregate([
            {
                $addFields: {
                  createdAtDate: { $toDate: '$created_at' }
                }
              },
              {
                $group: {
                  _id: {
                    year: { $year: '$createdAtDate' },
                    month: { $month: '$createdAtDate' },
                    day: { $dayOfMonth: '$createdAtDate' }
                  },
                  count: { $sum: 1 }
                }
              },
              { $sort: { '_id.year': 1, '_id.month': 1, '_id.day': 1 } }
        ]).toArray();

        const monthlyAdded = await db.collection('shopifyCustomers')
        .aggregate([
            {
                $addFields: {
                  createdAtDate: { $toDate: '$created_at' }
                }
              },
              {
                $group: {
                  _id: {
                    year: { $year: '$createdAtDate' },
                    month: { $month: '$createdAtDate' }
                  },
                  count: { $sum: 1 }
                }
              },
              { $sort: { '_id.year': 1, '_id.month': 1 } }
        ]).toArray();

     

        const quarterlyAdded = await db.collection('shopifyCustomers')
.aggregate([
    {
        $addFields: {
            createdAtDate: { $toDate: '$created_at' }
        }
    },
    {
        $group: {
            _id: {
                year: { $year: '$createdAtDate' },
                quarter: {
                    $ceil: {
                        $divide: [{ $month: '$createdAtDate' }, 3]
                    }
                }
            },
            count: { $sum: 1 }
        }
    },
    { $sort: { '_id.year': 1, '_id.quarter': 1 } }
]).toArray();

const yearlyAdded = await db.collection('shopifyCustomers')
.aggregate([
    {
        $addFields: {
            createdAtDate: { $toDate: '$created_at' }
        }
    },
    {
        $group: {
            _id: { year: { $year: '$createdAtDate' } },
            count: { $sum: 1 }
        }
    },
    { $sort: { '_id.year': 1 } }
]).toArray();

        res.json({ dailyAdded, monthlyAdded ,quarterlyAdded ,yearlyAdded });

    } catch(error){
        console.error('Error calculating total customers:', error)
    }
})


//total number of repeat customers
app.get('/api/noofreapeatingcustomers' ,async(req,res)=>{

    try{
        const db = client.db('RQ_Analytics');
    const dailyRepeatCustomers = await db.collection('shopifyOrders')
    .aggregate([
      {
        $addFields: {
          createdAtDate: { $toDate: '$created_at' }
        }
      },
      {
        $group: {
          _id: {
            year: { $year: '$createdAtDate' },
            month: { $month: '$createdAtDate' },
            day: { $dayOfMonth: '$createdAtDate' },
            customer_id: '$customer.id'
          },
          count: { $sum: 1 }
        }
      },
      {
        $match: {
          count: { $gt: 1 }
        }
      },
      {
        $group: {
          _id: {
            year: '$_id.year',
            month: '$_id.month',
            day: '$_id.day'
          },
          repeatCustomers: { $sum: 1 }
        }
      },
      { $sort: { '_id.year': 1, '_id.month': 1, '_id.day': 1 } }
    ])
    .toArray();


       // Aggregate repeat customers
       const monthlyRepeatCustomers = await db.collection('shopifyOrders')
       .aggregate([
         {
           $addFields: {
             createdAtDate: { $toDate: '$created_at' }
           }
         },
         {
           $group: {
             _id: {
               year: { $year: '$createdAtDate' },
               month: { $month: '$createdAtDate' },
               day: { $dayOfMonth: '$createdAtDate' },
               customer_id: '$customer.id'
             },
             count: { $sum: 1 }
           }
         },
         {
           $match: {
             count: { $gt: 1 }
           }
         },
         {
           $group: {
             _id: {
               year: '$_id.year',
               month: '$_id.month',
               day: '$_id.day'
             },
             repeatCustomers: { $sum: 1 }
           }
         },
         { $sort: { '_id.year': 1, '_id.month': 1, '_id.day': 1 } }
       ])
       .toArray();

          // Aggregate repeat customers
    const quarterlyRepeatCustomers = await db.collection('shopifyOrders')
    .aggregate([
      {
        $addFields: {
          createdAtDate: { $toDate: '$created_at' }
        }
      },
      {
        $group: {
          _id: {
            year: { $year: '$createdAtDate' },
            month: { $month: '$createdAtDate' },
            day: { $dayOfMonth: '$createdAtDate' },
            customer_id: '$customer.id'
          },
          count: { $sum: 1 }
        }
      },
      {
        $match: {
          count: { $gt: 1 }
        }
      },
      {
        $group: {
          _id: {
            year: '$_id.year',
            month: '$_id.month',
            day: '$_id.day'
          },
          repeatCustomers: { $sum: 1 }
        }
      },
      { $sort: { '_id.year': 1, '_id.month': 1, '_id.day': 1 } }
    ])
    .toArray();


    const yearlyRepeatCustomers = await db.collection('shopifyOrders')
    .aggregate([
      {
        $addFields: {
          createdAtDate: { $toDate: '$created_at' }
        }
      },
      {
        $group: {
          _id: {
            year: { $year: '$createdAtDate' },
            month: { $month: '$createdAtDate' },
            day: { $dayOfMonth: '$createdAtDate' },
            customer_id: '$customer.id'
          },
          count: { $sum: 1 }
        }
      },
      {
        $match: {
          count: { $gt: 1 }
        }
      },
      {
        $group: {
          _id: {
            year: '$_id.year',
            month: '$_id.month',
            day: '$_id.day'
          },
          repeatCustomers: { $sum: 1 }
        }
      },
      { $sort: { '_id.year': 1, '_id.month': 1, '_id.day': 1 } }
    ])
    .toArray();

   res.json({dailyRepeatCustomers,monthlyRepeatCustomers,quarterlyRepeatCustomers,yearlyRepeatCustomers})
   

    } catch(error){
        console.error('Error calculating total customers:', error)
    }
})

//customer location
app.get('/api/geolocation',async (req,res)=>{
    try {
        const db = client.db('RQ_Analytics');

        const customerLocations = await db.collection('shopifyCustomers')
          .aggregate([
            {
              $match: {
                'default_address.city': { $ne: null }
              }
            },
            {
              $group: {
                _id: {
                  city: '$default_address.city',
                  province: '$default_address.province',
                  country: '$default_address.country'
                },
                customerCount: { $sum: 1 }
              }
            }
          ])
          .toArray();
          console.log(customerLocations)
          res.json({customerLocations})
        } catch (error) {
          console.error('Error fetching customer locations:', error);
        }
})


//customer cohorts
app.get('/api/customer-cohorts', async (req, res) => {
  try {
      const db = client.db("RQ_Analytics");
      const ordersCollection = db.collection("shopifyOrders");
      const customersCollection = db.collection("shopifyCustomers");

      const customerFirstPurchase = await ordersCollection.aggregate([
          {
              $group: {
                  _id: "$customer.id",
                  firstPurchaseDate: { $min: { $toDate: "$created_at" } }
              }
          }
      ]).toArray();

      const cohorts = {};
      customerFirstPurchase.forEach(customer => {
          const cohortKey = moment(customer.firstPurchaseDate).format('YYYY-MM');
          if (!cohorts[cohortKey]) {
              cohorts[cohortKey] = [];
          }
          cohorts[cohortKey].push(customer._id);
      });

      const cohortLTV = await Promise.all(Object.entries(cohorts).map(async ([cohortKey, customerIds]) => {
          const totalRevenue = await ordersCollection.aggregate([
              {
                  $match: {
                      "customer.id": { $in: customerIds }
                  }
              },
              {
                  $group: {
                      _id: null,
                      total: { $sum: { $toDouble: "$total_price" } }
                  }
              }
          ]).toArray();

          const ltv = totalRevenue.length > 0 ? totalRevenue[0].total / customerIds.length : 0;

          return {
              cohort: cohortKey,
              customerCount: customerIds.length,
              ltv: ltv
          };
      }));

      cohortLTV.sort((a, b) => moment(a.cohort).diff(moment(b.cohort)));

      res.json(cohortLTV);
  } catch (error) {
      console.error('Error:', error);
      res.status(500).json({ error: 'An error occurred' });
  } 
});



//sales groewth rate  
app.get('/api/salesovertime', async (req, res) => {
  
    try{
        const db = client.db('RQ_Analytics');
        const dailySales = await db.collection('shopifyOrders')
        .aggregate([
          {
            $addFields: {
              createdAtDate: { $toDate: '$created_at' }
            }
          },
          {
            $group: {
              _id: {
                year: { $year: '$createdAtDate' },
                month: { $month: '$createdAtDate' },
                day: { $dayOfMonth: '$createdAtDate' }
              },
              totalPrice: { $sum: { $toDouble: '$total_price' } }
            }
          },
          { $sort: { '_id.year': 1, '_id.month': 1, '_id.day': 1 } }
        ])
        .toArray();

        const monthlySales = await db.collection('shopifyOrders')
      .aggregate([
        {
          $addFields: {
            createdAtDate: { $toDate: '$created_at' }
          }
        },
        {
          $group: {
            _id: {
              year: { $year: '$createdAtDate' },
              month: { $month: '$createdAtDate' }
            },
            totalPrice: { $sum: { $toDouble: '$total_price' } }
          }
        },
        { $sort: { '_id.year': 1, '_id.month': 1 } }
      ])
      .toArray();
  
      const quarterlySales = await db.collection('shopifyOrders')
      .aggregate([
        {
          $addFields: {
            createdAtDate: { $toDate: '$created_at' }
          }
        },
        {
          $group: {
            _id: {
              year: { $year: '$createdAtDate' },
              quarter: { $ceil: { $divide: [{ $month: '$createdAtDate' }, 3] } }
            },
            totalPrice: { $sum: { $toDouble: '$total_price' } }
          }
        },
        { $sort: { '_id.year': 1, '_id.quarter': 1 } }
      ])
      .toArray();
  
      const yearlySales = await db.collection('shopifyOrders')
      .aggregate([
        {
          $addFields: {
            createdAtDate: { $toDate: '$created_at' }
          }
        },
        {
          $group: {
            _id: { $year: '$createdAtDate' },
            totalPrice: { $sum: { $toDouble: '$total_price' } }
          }
        },
        { $sort: { '_id': 1 } }
      ])
      .toArray();

        const calculateGrowthRates = (salesData) => {
            const growthRates = [];

            for (let i = 1; i < salesData.length; i++) {
                const prevSales = salesData[i - 1].totalPrice;
                const currentSales = salesData[i].totalPrice;
                const growthRate = ((currentSales - prevSales) / prevSales) * 100;

                growthRates.push({
                    period: salesData[i]._id,
                    growthRate: growthRate.toFixed(2)
                });
            }

            return growthRates;
        };

        const dailyGrowthRates = calculateGrowthRates(dailySales);
        const monthlyGrowthRates = calculateGrowthRates(monthlySales);
        const quarterlyGrowthRates = calculateGrowthRates(quarterlySales);
        const yearlyGrowthRates = calculateGrowthRates(yearlySales);
      
        res.json({
            dailyGrowthRates,
            monthlyGrowthRates,
            quarterlyGrowthRates,
            yearlyGrowthRates
        });
    
    } catch(error){
        console.error('Error calculating total sales:', error)
    }  
})


app.listen(port,()=>{
    console.log(`server started on https://localhost:${port}`)
})