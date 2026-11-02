// Header names are matched case-insensitively. Service headers are lowercase (HTTP/2
// requires lowercase field names), but producers may still send other casings.

export function getHeader(headers: Record<string, string>, name: string): string | undefined {
  if (name in headers) return headers[name];
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(headers)) {
    if (k.toLowerCase() === lower) return v;
  }
  return undefined;
}

export function withoutHeader(headers: Record<string, string>, name: string): Record<string, string> {
  const lower = name.toLowerCase();
  return Object.fromEntries(Object.entries(headers).filter(([k]) => k.toLowerCase() !== lower));
}
