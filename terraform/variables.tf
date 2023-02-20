locals {
  envs = { for tuple in regexall("(.*)=([^\\n\\r]*)", file("../.pyenv")) : tuple[0] => tuple[1] }
}
