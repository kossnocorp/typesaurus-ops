import {
  add,
  all,
  collection,
  order,
  remove,
  startAfter,
  where
} from 'typesaurus'
import { processQuery } from '.'

describe('processQuery', () => {
  type User = { name: string; age: number }
  const users = collection<User>('users')

  beforeEach(async () => {
    await all(users).then((allUsers) =>
      Promise.all(allUsers.map((user) => remove(user.ref)))
    )

    await Promise.all([
      add(users, { name: 'Tati', age: 32 }),
      add(users, { name: 'Sasha', age: 34 }),
      add(users, { name: 'Sasha', age: 2 })
    ])
  })

  it('process each document in a query', async () => {
    const process = jest.fn()

    await processQuery(users, {
      queries: (last) => [
        where('name', '==', 'Sasha'),
        order('age', 'asc', last ? [startAfter(last.data.age)] : undefined)
      ],

      process
    })

    expect(process).toBeCalledTimes(2)
    expect(process.mock.calls.map(([doc]) => doc.data)).toEqual([
      { name: 'Sasha', age: 2 },
      { name: 'Sasha', age: 34 }
    ])
  })

  it('paginates queries', async () => {
    const queries = jest.fn((last) => [
      where('name', '==', 'Sasha'),
      order('age', 'asc', last ? [startAfter(last.data.age)] : undefined)
    ])

    const process = jest.fn()

    await processQuery(users, { queries, process, pageSize: 1 })

    expect(process).toBeCalledTimes(2)
    expect(queries).toBeCalledTimes(3)
    expect(queries.mock.calls.map(([doc]) => doc?.data)).toEqual([
      undefined,
      { name: 'Sasha', age: 2 },
      { name: 'Sasha', age: 34 }
    ])
  })

  it('allows to timeout processing', async () => {
    const process = jest.fn(
      (_) => new Promise((resolve) => setTimeout(resolve, 50))
    )

    await processQuery(users, {
      queries: (last) => [
        where('name', '==', 'Sasha'),
        order('age', 'asc', last ? [startAfter(last.data.age)] : undefined)
      ],

      process,

      timeout: 50,

      concurrency: 1
    })

    expect(process).toBeCalledTimes(1)
    expect(process.mock.calls.map(([doc]) => doc.data)).toEqual([
      { name: 'Sasha', age: 2 }
    ])
  })
})
