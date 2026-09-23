import { parse, toSeconds } from 'iso8601-duration';

export function parseDurationToMs(isoDuration: string): number {
  const parsed = parse(isoDuration);
  return toSeconds(parsed) * 1000;
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
