import type { Schema } from '@electric-lite/protocol'

export const schema: Schema = {
  tables: {
    todos: {
      columns: {
        id: { type: 'int' },
        title: { type: 'text' },
        priority: { type: 'int' },
        done: { type: 'bool' },
      },
      primaryKey: 'id',
    },
  },
}
