const crypto = require('crypto');

// Lazy import AWS SDK only when needed
let S3Client, PutObjectCommand, SESClient, SendEmailCommand, DynamoDBClient, PutItemCommand;
try {
  ({ S3Client, PutObjectCommand } = require('@aws-sdk/client-s3'));
  ({ SESClient, SendEmailCommand } = require('@aws-sdk/client-ses'));
  ({ DynamoDBClient, PutItemCommand } = require('@aws-sdk/client-dynamodb'));
} catch (e) {
  // Dependency may not be installed locally; Vercel will install on deploy
}

function uuid() {
  if (crypto.randomUUID) return crypto.randomUUID();
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(body));
}

function ok(res, body) {
  json(res, 200, body);
}

function getRawBody(req) {
  return new Promise((resolve, reject) => {
    let data = [];
    req.on('data', chunk => data.push(chunk));
    req.on('end', () => resolve(Buffer.concat(data)));
    req.on('error', reject);
  });
}

function slug(val, fallback = 'na') {
  if (!val) return fallback;
  const s = String(val).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
  return s || fallback;
}

module.exports = async (req, res) => {
  // CORS (allow same-origin and simple cross-origin posts)
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(204).end();

  if (req.method !== 'POST') {
    return json(res, 405, { error: 'Method Not Allowed' });
  }

  let payload = {};
  try {
    // Vercel may already parse JSON body for us
    if (req.body && typeof req.body === 'object') {
      payload = req.body;
    } else {
      const raw = await getRawBody(req);
      const text = raw.toString('utf8') || '';
      if (text && req.headers['content-type'] && req.headers['content-type'].includes('application/json')) {
        payload = JSON.parse(text);
      } else if (text) {
        // Try URL-encoded fallback
        payload = Object.fromEntries(new URLSearchParams(text));
      }
    }
  } catch (e) {
    // ignore, payload stays {}
  }

  // Attach server-side metadata
  const receivedAt = new Date();
  const clientId = (payload && typeof payload.id === 'string' && payload.id) || undefined;
  const status = (payload && typeof payload.status === 'string' && payload.status.toLowerCase()) || 'final';
  const doc = {
    id: clientId || uuid(),
    status,
    received_at: receivedAt.toISOString(),
    ip: (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').toString(),
    user_agent: req.headers['user-agent'] || '',
    ...payload,
  };

  // Attempt to store in S3 if configured
  const bucket = process.env.AWS_S3_BUCKET;
  const region = process.env.AWS_REGION;
  const prefix = process.env.AWS_S3_PREFIX || 'leads';
  let stored = 'none';
  let key = '';

  if (bucket && region && S3Client && PutObjectCommand) {
    try {
      const datePart = receivedAt.toISOString().slice(0, 10); // YYYY-MM-DD
      const statusPart = slug(status || 'final');
      const source = slug((payload && (payload.source || (payload.data && payload.data.source))) || 'web');
      const industry = slug((payload && payload.data && payload.data.industry) || 'na');
      const utmSource = slug((payload && payload.data && payload.data.utm_data && payload.data.utm_data.utm_source) || 'direct');
      key = `${prefix}/${statusPart}/${datePart}/source=${source}/utm=${utmSource}/industry=${industry}/${doc.id}.json`;
      const s3 = new S3Client({ region });
      await s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: Buffer.from(JSON.stringify(doc, null, 2)),
        ContentType: 'application/json',
      }));
      stored = 's3';
    } catch (err) {
      // Fall through; still return OK so UX is not blocked
      stored = 'error';
      key = '';
      console.error('S3 write error:', err);
    }
  }

  // Optional DynamoDB index write (summary item)
  try {
    const table = process.env.DDB_TABLE;
    const ddbRegion = process.env.AWS_REGION || process.env.DDB_REGION;
    if (table && ddbRegion && DynamoDBClient && PutItemCommand) {
      const data = payload && payload.data ? payload.data : payload;
      const ddb = new DynamoDBClient({ region: ddbRegion });
      const item = {
        id: { S: doc.id },
        status: { S: String(doc.status || 'final') },
        received_at: { S: doc.received_at },
        email: { S: String((data && data.email) || '') },
        utm_source: { S: String((data && data.utm_data && data.utm_data.utm_source) || 'direct') },
        s3_key: { S: String(key || '') },
        first_name: { S: String((data && data.first_name) || '') },
        last_name: { S: String((data && data.last_name) || '') },
        phone: { S: String((data && data.phone) || '') },
        company: { S: String((data && (data.name || data.company_name)) || '') },
        website: { S: String((data && data.website) || '') },
        industry: { S: String((data && data.industry) || '') }
      };
      await ddb.send(new PutItemCommand({ TableName: table, Item: item }));
    }
  } catch (err) {
    console.error('DynamoDB put error:', err);
  }

  // Optional SES email on final submissions
  try {
    const sesTo = process.env.SES_TO;
    const sesFrom = process.env.SES_FROM;
    const sesRegion = process.env.SES_REGION || region;
    if (status === 'final' && sesTo && sesFrom && sesRegion && SESClient && SendEmailCommand) {
      const ses = new SESClient({ region: sesRegion });
      const company = (payload && payload.data && (payload.data.name || payload.data.company_name)) || 'Unknown Company';
      const first = (payload && payload.data && payload.data.first_name) || '';
      const last = (payload && payload.data && payload.data.last_name) || '';
      const subject = `New Lead: ${company} – ${first} ${last}`.trim();
      const bodyText = `New lead captured (status: ${status}).\n\nKey: ${key || '(none)'}\nID: ${doc.id}\nTime: ${doc.received_at}\nIP: ${doc.ip}\nUA: ${doc.user_agent}\n\nData:\n${JSON.stringify(payload.data || payload, null, 2)}`;
      await ses.send(new SendEmailCommand({
        Destination: { ToAddresses: [sesTo] },
        Source: sesFrom,
        Message: {
          Subject: { Data: subject },
          Body: {
            Text: { Data: bodyText },
          }
        }
      }));
    }
  } catch (err) {
    console.error('SES send error:', err);
  }

  // Optional CRM syncs (final only)
  if (status === 'final') {
    const data = payload && payload.data ? payload.data : payload;
    const first = (data && data.first_name) || '';
    const last = (data && data.last_name) || '';
    const email = (data && data.email) || '';
    const phone = (data && data.phone) || '';
    const company = (data && (data.name || data.company_name)) || '';
    const annual = (data && data.annual_sales) || '';
    const country = (data && data.country) || '';

    // HubSpot
    if (process.env.HUBSPOT_TOKEN) {
      try {
        const properties = { email, firstname: first, lastname: last };
        if (phone) properties.phone = phone;
        if (company) properties.company = company;
        if (annual) properties.annualrevenue = String(annual);
        if (country) properties.country = String(country);
        await fetch('https://api.hubapi.com/crm/v3/objects/contacts', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${process.env.HUBSPOT_TOKEN}`
          },
          body: JSON.stringify({ properties })
        });
      } catch (err) {
        console.error('HubSpot sync error:', err);
      }
    }

    // Close.com
    if (process.env.CLOSE_API_KEY) {
      try {
        const auth = Buffer.from(process.env.CLOSE_API_KEY + ':').toString('base64');
        const body = {
          name: company || `${first} ${last}`.trim() || 'Lead',
          contacts: [{
            name: `${first} ${last}`.trim() || undefined,
            emails: email ? [{ email }] : [],
            phones: phone ? [{ phone }] : [],
          }],
          custom: {
            source: payload && payload.source ? payload.source : 'stripeprotection-get-started'
          }
        };
        await fetch('https://api.close.com/api/v1/lead/', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Basic ${auth}`
          },
          body: JSON.stringify(body)
        });
      } catch (err) {
        console.error('Close sync error:', err);
      }
    }
  }

  return ok(res, { ok: true, stored, key, id: doc.id, status: doc.status });
};
