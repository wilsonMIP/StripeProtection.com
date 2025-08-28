// Lists recent leads from S3 and returns JSON for admin use.
let { S3Client, ListObjectsV2Command, GetObjectCommand } = (() => {
  try { return require('@aws-sdk/client-s3'); } catch { return {}; }
})();

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(body));
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString('utf8');
}

module.exports = async (req, res) => {
  // Basic CORS
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

  const bucket = process.env.AWS_S3_BUCKET;
  const region = process.env.AWS_REGION;
  const prefix = process.env.AWS_S3_PREFIX || 'leads';
  if (!bucket || !region || !S3Client) {
    return json(res, 200, { items: [], note: 'S3 not configured' });
  }

  const status = (url.searchParams.get('status') || 'final').toLowerCase();
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '50', 10) || 50, 200);

  try {
    const s3 = new S3Client({ region });
    const list = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: `${prefix}/${status}/`,
      MaxKeys: 1000,
    }));

    const contents = (list.Contents || []).filter(o => o.Key && o.Key.endsWith('.json'));
    contents.sort((a, b) => new Date(b.LastModified || 0) - new Date(a.LastModified || 0));
    const top = contents.slice(0, limit);

    const items = [];
    for (const obj of top) {
      try {
        const got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: obj.Key }));
        const text = got.Body && got.Body.transformToString ? await got.Body.transformToString() : await streamToString(got.Body);
        const parsed = JSON.parse(text);
        items.push(parsed);
      } catch (e) {
        // skip
      }
    }

    return json(res, 200, { items });
  } catch (err) {
    return json(res, 500, { error: 'Failed to list leads' });
  }
};

