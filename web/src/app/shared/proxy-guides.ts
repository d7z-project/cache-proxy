import { InstanceSummary, ProxyMode } from '../core/api.models';

export interface InstanceGuideSnippet {
  label: string;
  code: string;
}

export interface InstanceGuide {
  entryLabel: string;
  entryUrl?: string;
  snippets: InstanceGuideSnippet[];
}

export function buildInstanceGuide(instance: InstanceSummary, origin: string): InstanceGuide {
  const entry = proxyEntry(instance, origin);
  switch (instance.mode) {
    case ProxyMode.Go:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          { label: 'go', code: `GOPROXY=${entry.url} go mod download` }
        ]
      };
    case ProxyMode.Maven:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          { label: 'Maven', code: `<repository>\n  <id>${instance.name}</id>\n  <url>${entry.url}</url>\n</repository>` },
          { label: 'Gradle', code: `repositories {\n  maven(url = "${entry.url}")\n}` }
        ]
      };
    case ProxyMode.Cargo:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          { label: 'Cargo', code: `[registries.${instance.name}]\nprotocol = "sparse"\nindex = "${entry.url}"` }
        ]
      };
    case ProxyMode.PyPI:
      return {
        entryLabel: entry.label,
        entryUrl: `${entry.url}/simple`,
        snippets: [
          { label: 'pip', code: `pip install -i ${entry.url}/simple requests` },
          { label: 'pip.conf', code: `[global]\nindex-url = ${entry.url}/simple` }
        ]
      };
    case ProxyMode.Npm:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          { label: 'npm', code: `npm config set registry ${entry.url}` }
        ]
      };
    case ProxyMode.Oci:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          { label: 'Docker', code: `docker pull ${stripScheme(entry.url)}/library/alpine:latest` }
        ]
      };
    default:
      return {
        entryLabel: entry.label,
        entryUrl: entry.url,
        snippets: [
          { label: 'curl', code: `curl ${entry.url}/example.tar.gz` }
        ]
      };
  }
}

function proxyEntry(instance: InstanceSummary, origin: string): { label: string; url: string } {
  if (instance.path) {
    return { label: '路径入口', url: origin + instance.path };
  }
  return { label: '独立端口', url: bindEntryURL(instance.bind ?? '', origin) };
}

function stripScheme(value: string): string {
  return value.replace(/^https?:\/\//, '');
}

function bindEntryURL(bind: string, origin: string): string {
  if (bind.startsWith('http://') || bind.startsWith('https://')) return bind;
  const parsed = parseBind(bind);
  if (!parsed) return `http://${bind}`;
  const base = new URL(origin);
  if (parsed.host === '0.0.0.0' || parsed.host === '::' || parsed.host === '[::]') {
    return `${base.protocol}//${base.hostname}:${parsed.port}`;
  }
  if (parsed.host === '127.0.0.1' || parsed.host === 'localhost') {
    return `http://${parsed.host}:${parsed.port}`;
  }
  const host = parsed.host.includes(':') && !parsed.host.startsWith('[') ? `[${parsed.host}]` : parsed.host;
  return `http://${host}:${parsed.port}`;
}

function parseBind(bind: string): { host: string; port: string } | undefined {
  const value = bind.trim();
  if (!value) return undefined;
  if (value.startsWith('[')) {
    const end = value.indexOf(']');
    if (end <= 0 || value[end + 1] !== ':') return undefined;
    return { host: value.slice(1, end), port: value.slice(end + 2) };
  }
  const index = value.lastIndexOf(':');
  if (index <= 0 || index === value.length - 1) return undefined;
  return { host: value.slice(0, index), port: value.slice(index + 1) };
}
