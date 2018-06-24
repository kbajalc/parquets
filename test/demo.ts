import 'jest';

describe('This is a demo test', () => {
  beforeAll(async () => {
    // TODO: Before all tests
  });

  beforeEach(async () => {
    // TODO: Before each test
  });

  afterAll(async () => {
    // TODO: After all tests
  });

  test('test', async () => {
    // LEARN:
    // chai
    // sinon-chai
    // chai-as-promised

    let a = 1;
    const b = a++;

    expect(a).not.toBe(b);
  });
});
