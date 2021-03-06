describe 'database' do
  before do
    `rm -f test.db`
  end

  def run_script(commands)
    raw_output = nil
    IO.popen("./db test.db", "r+") do |pipe|
      commands.each do |cmd|
        pipe.puts cmd
      end

      pipe.close_write

      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'inserts and retrieves a row' do
    result = run_script([
      'insert 1 user1 person1@example.com',
      'select',
      '.exit',
    ])
    expect(result).to match_array([
      'db > Executed.',
      'db > (1, user1, person1@example.com)',
      'Executed.',
      'db > ',
    ])
  end

  it 'prints error message when table is full' do
    MAX_PAGE_NUM = 100.freeze
    ROWS_IN_PAGE = 14.freeze
    script = (1..(MAX_PAGE_NUM* ROWS_IN_PAGE + 1)).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << '.exit'
    result = run_script(script)
    expect(result[-2]).to eq('db > Error: Table full.')
  end

  it 'allows inserting strings that are maximum length' do
    long_username = 'u' * 32
    long_email = 'e' * 255
    script = [
      "insert 1 #{long_username} #{long_email}",
      'select',
      '.exit',
    ]
    result = run_script(script)
    expect(result).to match_array([
      'db > Executed.',
      "db > (1, #{long_username}, #{long_email})",
      'Executed.',
      'db > ',
    ])
  end

  it 'prints an error message when strings are too long' do
    long_username = 'u' * 33
    long_email = 'e' * 256
    script = [
      "insert 1 #{long_username} #{long_email}",
      'select',
      '.exit',
    ]
    result = run_script(script)
    expect(result).to match_array([
      'db > String is too long.',
      'db > Executed.',
      'db > ',
    ])
  end

  it 'prints an erorr message when id is negative' do
    script = [
      'insert -1 0gajun 0gajun@example.com',
      'select',
      '.exit',
    ]
    result = run_script(script)
    expect(result).to match_array([
      'db > ID must be positive.',
      'db > Executed.',
      'db > ',
    ])
  end

  it 'keeps data after closing connection' do
    result1 = run_script([
      'insert 1 user1 person1@example.com',
      '.exit',
    ])
    expect(result1).to match_array([
      'db > Executed.',
      'db > ',
    ])
    result2 = run_script([
      'select',
      '.exit',
    ])
    expect(result2).to match_array([
      'db > (1, user1, person1@example.com)',
      'Executed.',
      'db > ',
    ])
  end

  it 'prints constants' do
    result = run_script([
      '.constants',
      '.exit',
    ])
    expect(result).to match_array([
      'db > Constants:',
      'ROW_SIZE: 293',
      'COMMON_NODE_HEADER_SIZE: 6',
      'LEAF_NODE_HEADER_SIZE: 10',
      'LEAF_NODE_CELL_SIZE: 297',
      'LEAF_NODE_SPACE_FOR_CELLS: 4086',
      'LEAF_NODE_MAX_CELLS: 13',
      'db > ',
    ])
  end

  it 'allows printing out the structure of a one-node btree' do
    script = [3, 1, 2].map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << '.btree'
    script << '.exit'
    result = run_script(script)

    expect(result).to match_array([
      'db > Executed.',
      'db > Executed.',
      'db > Executed.',
      'db > Tree:',
      '- leaf (size 3)',
      "\t- 1",
      "\t- 2",
      "\t- 3",
      'db > ',
    ])
  end

  it 'printa an error message if there is a duplicate id' do
    result = run_script([
      'insert 1 user1 person1@example.com',
      'insert 1 user1 person1@example.com',
      'select',
      '.exit',
    ])
    expect(result).to match_array([
      'db > Executed.',
      'db > Error: Duplicate key.',
      'db > (1, user1, person1@example.com)',
      'Executed.',
      'db > ',
    ])
  end

  it 'allows printing out the structure of a 3-leaf-node btree' do
    script = (1..14).map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << '.btree'
    script << 'insert 15 user15 person15@example.com'
    script << '.exit'

    result = run_script(script)

    expect(result[14...(result.length)]).to match_array([
      'db > Tree:',
      '- internal (size 1)',
      "\t- leaf (size 7)",
      "\t\t- 1",
      "\t\t- 2",
      "\t\t- 3",
      "\t\t- 4",
      "\t\t- 5",
      "\t\t- 6",
      "\t\t- 7",
      "\t- key 7",
      "\t- leaf (size 7)",
      "\t\t- 8",
      "\t\t- 9",
      "\t\t- 10",
      "\t\t- 11",
      "\t\t- 12",
      "\t\t- 13",
      "\t\t- 14",
      'db > Need to implement searching an internal node'
    ])
  end
end
