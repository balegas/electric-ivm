// Build the engine binary once before any test worker starts, so parallel workers don't race
// on the cargo build lock. Sets a flag the harness reads to skip its own per-worker build.
import { execFileSync } from 'node:child_process'

export default function setup() {
  execFileSync('cargo', ['build', '-p', 'electric-lite-engine'], { stdio: 'inherit' })
  process.env.ELECTRIC_LITE_ENGINE_PREBUILT = '1'
}
