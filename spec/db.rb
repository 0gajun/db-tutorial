describe 'database' do
  def run_script(commands)
    raw_output = nil
    IO.popen("./db", "r+") do |pipe|
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
end
