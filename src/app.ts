import fs from 'node:fs'
import path from 'node:path'

const parseDotEnv = (content: string): Record<string, string> => {
  const out: Record<string, string> = {}
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim()
    if (!line || line.startsWith('#')) continue

    const idx = line.indexOf('=')
    if (idx <= 0) continue

    const key = line.slice(0, idx).trim()
    let value = line.slice(idx + 1).trim()
    if (!key) continue

    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1)
    }

    out[key] = value
  }
  return out
}

const ensureRootNpmConfigStub = () => {
  const pkgPath = path.join(process.cwd(), 'package.json')
  if (!fs.existsSync(pkgPath)) return

  const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8')) as {
    name?: string
    karin?: { web?: string }
  }
  if (!pkg.name || !pkg.karin?.web) return

  const stubDir = path.join(process.cwd(), 'node_modules', pkg.name)
  fs.mkdirSync(stubDir, { recursive: true })

  fs.copyFileSync(pkgPath, path.join(stubDir, 'package.json'))

  for (const dirName of ['lib', 'config'] as const) {
    const src = path.join(process.cwd(), dirName)
    if (!fs.existsSync(src)) continue
    fs.cpSync(src, path.join(stubDir, dirName), { recursive: true, force: true })
  }
}

try {
  const envFile = process.env.EBV_FILE || '.env'
  const envPath = path.resolve(process.cwd(), envFile)
  const env = fs.existsSync(envPath) ? parseDotEnv(fs.readFileSync(envPath, 'utf8')) : {}
  if ((env.NODE_ENV || '').trim() !== 'development') {
    ensureRootNpmConfigStub()
  }
} catch {
  // ignore
}

import('node-karin/start')
