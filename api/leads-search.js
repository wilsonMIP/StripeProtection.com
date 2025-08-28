// Query DynamoDB GSIs for fast lead search/filtering
let { DynamoDBClient, QueryCommand } = (() => {
  try { return require('@aws-sdk/client-dynamodb'); } catch { return {}; }
})();

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(body));
}

module.exports = async (req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-admin-key');
  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'GET') return json(res, 405, { error: 'Method Not Allowed' });

  const url = new URL(req.url, `http://${req.headers.host}`);
  const keyParam = url.searchParams.get('key') || req.headers['x-admin-key'];
  const requiredKey = process.env.ADMIN_TOKEN;
  if (!requiredKey || keyParam !== requiredKey) {
    return json(res, 401, { error: 'Unauthorized' });
  }

  const region = process.env.AWS_REGION;
  const table = process.env.DDB_TABLE || 'StripeProtectionLeads';
  if (!region || !table || !DynamoDBClient) {
    return json(res, 500, { error: 'DynamoDB not configured' });
  }

  const status = (url.searchParams.get('status') || '').trim();
  const email = (url.searchParams.get('email') || '').trim();
  const utm = (url.searchParams.get('utm_source') || '').trim();
  const from = (url.searchParams.get('from') || '').trim(); // ISO start
  const to = (url.searchParams.get('to') || '').trim(); // ISO end
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '50', 10) || 50, 200);
  const nextToken = url.searchParams.get('nextToken');

  const ddb = new DynamoDBClient({ region });

  // Build primary query using one index; allow additional filters as FilterExpression
  let IndexName = undefined;
  let KeyConditionExpression = undefined;
  let ExpressionAttributeValues = {};
  let ExpressionAttributeNames = {};
  let FilterExpressionParts = [];

  const dateFrom = from || '0000-01-01T00:00:00.000Z';
  const dateTo = to || '9999-12-31T23:59:59.999Z';

  if (email) {
    IndexName = 'byEmail';
    KeyConditionExpression = '#E = :e AND #R BETWEEN :from AND :to';
    ExpressionAttributeNames['#E'] = 'email';
    ExpressionAttributeNames['#R'] = 'received_at';
    ExpressionAttributeValues[':e'] = { S: email };
    ExpressionAttributeValues[':from'] = { S: dateFrom };
    ExpressionAttributeValues[':to'] = { S: dateTo };
    if (status) { FilterExpressionParts.push('#S = :s'); ExpressionAttributeNames['#S'] = 'status'; ExpressionAttributeValues[':s'] = { S: status }; }
    if (utm) { FilterExpressionParts.push('#U = :u'); ExpressionAttributeNames['#U'] = 'utm_source'; ExpressionAttributeValues[':u'] = { S: utm }; }
  } else if (status) {
    IndexName = 'byStatusDate';
    KeyConditionExpression = '#S = :s AND #R BETWEEN :from AND :to';
    ExpressionAttributeNames['#S'] = 'status';
    ExpressionAttributeNames['#R'] = 'received_at';
    ExpressionAttributeValues[':s'] = { S: status };
    ExpressionAttributeValues[':from'] = { S: dateFrom };
    ExpressionAttributeValues[':to'] = { S: dateTo };
    if (utm) { FilterExpressionParts.push('#U = :u'); ExpressionAttributeNames['#U'] = 'utm_source'; ExpressionAttributeValues[':u'] = { S: utm }; }
    if (email) { FilterExpressionParts.push('#E = :e'); ExpressionAttributeNames['#E'] = 'email'; ExpressionAttributeValues[':e'] = { S: email }; }
  } else if (utm) {
    IndexName = 'byUtmSource';
    KeyConditionExpression = '#U = :u AND #R BETWEEN :from AND :to';
    ExpressionAttributeNames['#U'] = 'utm_source';
    ExpressionAttributeNames['#R'] = 'received_at';
    ExpressionAttributeValues[':u'] = { S: utm };
    ExpressionAttributeValues[':from'] = { S: dateFrom };
    ExpressionAttributeValues[':to'] = { S: dateTo };
    if (status) { FilterExpressionParts.push('#S = :s'); ExpressionAttributeNames['#S'] = 'status'; ExpressionAttributeValues[':s'] = { S: status }; }
  } else {
    // Fallback: must supply at least one of email/status/utm for indexed query
    return json(res, 400, { error: 'Provide at least one of: email, status, utm_source' });
  }

  const params = {
    TableName: table,
    IndexName,
    KeyConditionExpression,
    ExpressionAttributeValues,
    ExpressionAttributeNames,
    Limit: limit
  };
  if (FilterExpressionParts.length) params.FilterExpression = FilterExpressionParts.join(' AND ');
  if (nextToken) {
    try { params.ExclusiveStartKey = JSON.parse(Buffer.from(nextToken, 'base64').toString('utf8')); } catch {}
  }

  try {
    const out = await ddb.send(new QueryCommand(params));
    const items = (out.Items || []).map(i => {
      const obj = {};
      for (const [k, v] of Object.entries(i)) obj[k] = v.S || v.N || v.BOOL || null;
      return obj;
    });
    const response = {
      count: out.Count || 0,
      items,
      nextToken: out.LastEvaluatedKey ? Buffer.from(JSON.stringify(out.LastEvaluatedKey), 'base64').toString('utf8') && Buffer.from(JSON.stringify(out.LastEvaluatedKey)).toString('base64') : null
    };
    return json(res, 200, response);
  } catch (err) {
    return json(res, 500, { error: 'Dynamo query failed' });
  }
};

