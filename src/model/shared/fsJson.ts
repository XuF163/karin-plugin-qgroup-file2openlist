import fs from 'node:fs'
import path from 'node:path'

export const ensureDir = (dirPath: string) => fs.mkdirSync(dirPath, { recursive: true })

export const readJsonSafe = (filePath: string): any => {
  try {
    if (!fs.existsSync(filePath)) return {}
    const raw = fs.readFileSync(filePath, 'utf8')
    return raw ? JSON.parse(raw) : {}
  } catch {
    return {}
  }
}

export const writeJsonSafe = (filePath: string, data: unknown) => {
  ensureDir(path.dirname(filePath))
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8')
}

