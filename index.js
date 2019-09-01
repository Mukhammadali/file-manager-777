const MongoClient = require('mongodb').MongoClient;
const MONGODB_URI = process.env.MONGODB_URI; // or Atlas connection string
const BUCKET_NAME =  process.env.BUCKET_NAME;
const REGION =  process.env.REGION ;
const AWS = require('aws-sdk')
const shortid = require('shortid');

AWS.config.update({ region: REGION })

const s3 = new AWS.S3({
  signatureVersion: 'v4',
});




let cachedDb = null;

function connectToDatabase (uri) {
  console.log('=> connect to database');

  if (cachedDb) {
    console.log('=> using cached database instance');
    return Promise.resolve(cachedDb);
  }

  return MongoClient.connect(uri, { useNewUrlParser: true, useUnifiedTopology: true })
    .then(client => {
      cachedDb = client.db();
      return cachedDb;
    });
}


exports.handler = async (event, context, callback) => {
  context.callbackWaitsForEmptyEventLoop = false;
  const parsedBody = JSON.parse(event.body);
  try {
    const db = await connectToDatabase(MONGODB_URI);
    const collection = db.collection('files');

    if(event.path === '/get' && event.httpMethod === 'GET'){
      if(parsedBody && parsedBody.file_name) {
        const result = await getFile(collection, {
          file_name: parsedBody.file_name
        });
        return sendResponse(200, {
          data: result
       })
      }
      return sendResponse(500, { error: 'Please make sure to provide  file_name!' })
    }
    if(event.path === '/list' && event.httpMethod === 'GET'){
      const result = await getFiles(collection);
      return sendResponse(200, {
        data: result
     })
    }
    if(event.path === '/create' && event.httpMethod === 'POST'){
      if(parsedBody && parsedBody.file_size && parsedBody.file_type){
        const result = await createFile(collection, {
          file_name: parsedBody.file_size,
          file_type: parsedBody.file_type
        });
        return sendResponse(200, {
          data: result
       })
      }
      return sendResponse(500, { error: 'Please make sure to provide file_type and file_size!' })
    }
    if(event.path === '/delete' && event.httpMethod === 'DELETE'){
      if(parsedBody && parsedBody.file_name) {
        const result = await deleteFile(collection, {
          file_name: parsedBody.file_name
        });
        return sendResponse(200, {
           data: result
        })
      }
      return sendResponse(500, { error: 'Please make sure to provide file_name!' })
    }
    return sendResponse(200, { data: 'It is working fine' });

  } catch (err){
    console.log(err);
    return sendResponse(500, err);
  }
};


async function getFile(collection, args) {
  const file = await collection.findOne(args);
  
  if(file) {
    // append s3 url
    file.url = await getDownloadURL(file.file_name)
    return file;
  }
  return;
}

async function getFiles(collection) {
  const files = await collection.find({}).toArray();
  if(files && files.length > 0) {
    // append s3 url to each file
    for (let file of files) {
      file.url = await getDownloadURL(file.file_name)
    }
    // sort files by created date
    const sortedByDate = files.sort((a, b) => {
      return (a.created_at < b.created_at) ? -1 : ((a.created_at > b.created_at) ? 1 : 0);
    });

    return sortedByDate;
  }
  return []
}


async function createFile(collection, { file_type, file_size }) {
  try {
    const randomId = shortid.generate()
    // get file ext from file_type, works for only some basic file types not for all
    const ext = file_type.split('/').pop();
    const file_name = `${randomId}.${ext}`;
    // save file with generated name
    const file = await collection.insertOne({
      file_name,
      created_at: new Date(Date.now())
    })
    if(file){
      // generate url for client-side to upload a file
      const url  = await getUploadURL({
        file_name,
        file_size,
        file_type
      });
      return url
    }
    return;
  } catch (err) {
    console.log('err:', err);
    return;
  }
}

async function deleteFile(collection, args) {
  try {
    // check if file exists in db
  const file = await collection.findOne(args);
  if(file){
    const params = {
      Bucket: BUCKET_NAME, 
      Key: args.file_name
     };
    // remove file from s3
    const isDeleted = await s3.deleteObject(params).promise();
    if(isDeleted){
      // remove file from db
      const isDeleted = await collection.remove({ file_name: args.file_name });
      if (isDeleted){
        return file;
      }
    }
    return;
  }
  return;
  } catch (err){
    console.log(err);
    return;
  }
}

function sendResponse(status, data) {
  var response = {
    statusCode: status,
    body: JSON.stringify(data)
  };
  return response;
}

const getUploadURL = async ({file_size, file_type, file_name}) => {
  const s3Params = {
    Bucket: BUCKET_NAME,
    ContentType: file_type,
    Expires: 60,
    Key: file_name,
  };
  
  return new Promise((resolve, reject) => {
    const url = s3.getSignedUrl('putObject', s3Params)
    resolve(url);
  })
}

const getDownloadURL = async (file_name) => {
  const s3Params = {
    Bucket: BUCKET_NAME,
    Expires: 86400,
    Key: file_name,
    ResponseContentDisposition: 'attachment',
  };
  
  return new Promise((resolve, reject) => {
    const url = s3.getSignedUrl('getObject', s3Params)
    resolve(url);
  })
}