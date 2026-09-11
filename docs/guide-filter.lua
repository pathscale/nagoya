-- Rustdoc hides # lines inside Rust examples. Hide the same lines in print.
function CodeBlock(block)
  local lines = {}
  for line in (block.text .. "\n"):gmatch("(.-)\n") do
    if not line:match("^# ") and line ~= "#" then
      table.insert(lines, line)
    end
  end
  block.text = table.concat(lines, "\n")
  if block.classes[1] == "no_run" then block.classes[1] = "rust" end
  return block
end
