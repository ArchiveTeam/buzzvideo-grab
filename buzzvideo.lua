dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false

local web_stream_count = 0
local api_params = {
  ac = "WIFI",
  dpi = "440",
  resolution = "1920*1080",
  channel = "pc",
  original_channel = "pc",
  aid = "1131",
  app_version = "10.6.0",
  version_code = "10600",
  update_version_code = "106000",
  manifest_version_code = "10600",
  device_type = "pc",
  brand = "dell",
  os = "windows",
  hevc_supported = "1",
  os_version = "10",
  tz_offset = "19800",
  tz_name = "Asia/Jakarta",
  youtube = "1",
  device_platform = "web",
  device_brand = "dell",
  language = "ja",
  region = "jp"
}
local api_params_string = ""
for k, v in pairs(api_params) do
  if string.len(api_params_string) > 0 then
    api_params_string = api_params_string .. "&"
  end
  api_params_string = api_params_string .. k .. "=" .. string.gsub(v, "/", "%%2F")
end

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('queuing' , item)
    target[item] = true
  end
end

find_item = function(url)
  local value = string.match(url, "^https?://www%.buzzvideo%.com/article/i([0-9]+)$")
  local type_ = "video"
  if not value then
    value = string.match(url, "^https?://www%.buzzvideo%.com/user/([0-9]+)$")
    type_ = "user"
  end
  if value then
    item_type = type_
    item_value = value
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[value] = true
      abortgrab = false
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  for s in string.gmatch(url, "([0-9]+)") do
    if ids[s] then
      return true
    end
  end

  for s in string.gmatch(url, "([0-9a-zA-Z_]+)") do
    if ids[s] then
      return true
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"]) and not processed(url) then
    addedtolist[url] = true
    return true
  end]]

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not allowed(url_, origurl) then
      error("URL unexpectedly not accepted.")
    end
    if not processed(url_)
      and allowed(url_, origurl) then
      if string.match(url_, "/globalv%-web/") and item_type == "video" then
        table.insert(urls, {
          url=url_,
          headers={
            ["Referer"]="https://www.vlive.tv/video/" .. item_type
          }
        })
      else
        table.insert(urls, { url=url_ })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function load_initial_state(html)
    local json = string.match(html, '<script>%s*window%.__INITIAL_STATE__=({.-})</script>')
    json = string.gsub(json, ":undefined", ":null")
    json = JSON:decode(json)
    return json
  end

  local function discover_from_json(json)
    for k, v in pairs(json) do
      if type(v) == "table" then
        discover_from_json(v)
      elseif k == "item_id" or k == "articleId" then
        discover_item(discovered_items, "video:" .. v)
      elseif k == "user_id" or k == "videoAuthorId" then
        discover_item(discovered_items, "user:" .. v)
      end
    end
  end

  local function force_queue(newurl)
    ids[newurl] = true
    check(newurl)
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "^https?://[^/]*topbuzzcdn%.com/")
    and not string.match(url, "^https?://[^/]*ipstatp%.com/") then
    html = read_file(file)
    if string.match(url, "^https?://[^/]+/user/") then
      local json = load_initial_state(html)
      local newurl = json["profileInfo"]["avatarUrl"]
      if newurl and string.len(newurl) > 0 then
        force_queue(newurl)
      end
      check("https://www.buzzvideo.com/user/" .. item_value .. "/publish")
      check("https://www.buzzvideo.com/user/" .. item_value .. "/following")
      check("https://www.buzzvideo.com/user/" .. item_value .. "/follower")
      check("https://www.buzzvideo.com/pgc/article/list?cursor=0&limit=10&user_id=" .. item_value)
      check("https://www.buzzvideo.com/pgc/fans/following_list?cursor=0&count=10&from_user_id=" .. item_value)
      check("https://www.buzzvideo.com/pgc/fans/followers_list?cursor=0&count=10&from_user_id=" .. item_value)
    end
    if string.match(url, "^https?://[^/]+/pgc/") then
      local json = JSON:decode(html)
      discover_from_json(json)
      if json["message"] ~= "success" then
        error("Got bad data from API.")
      end
      if tonumber(json["data"]["cursor"]) ~= 0 then
        local newurl = string.gsub(url, "(cursor=)[0-9]+", "%1" .. tostring(json["data"]["cursor"]))
        check(newurl)
      end
      --[[if string.match(url, "/pgc/article/list%?") then
        for _, item_data in pairs(json["data"]["items"]) do
          for _, cover_data in pairs(item_data["pgc_feed_covers"]) do
            force_queue("https://p16-va.topbuzzcdn.com/list/" .. cover_data["web_uri"])
          end
          force_queue("https://p16-va.topbuzzcdn.com/list/" .. item_data["video_info"]["video_thumbnail"]["web_uri"])
        end
      end]]
    end
    if item_type == "video"
      and (
        string.match(url, "^https?://[^/]*buzzvideo%.com/@")
        or string.match(html, "__INITIAL_STATE__")
      ) then
      local json = load_initial_state(html)
      check(
        "https://www.buzzvideo.com/api/777/web/stream"
        .. "?category_parameter=" .. item_value
        .. "&count=30"
        .. "&max_behot_time=0"
        .. "&category=13"
        .. "&session_impr_id=0"
        .. "&" .. api_params_string
      )
      check(
        "https://www.buzzvideo.com/api/1200/web/comment_v2/comments"
        .. "?" .. string.gsub(api_params_string, "(&aid=)[0-9]+", "%16816")
        .. "&item_id=" .. json["story"]["articleId"]
        .. "&group_id=" .. json["story"]["groupId"]
        .. "&media_id=" .. json["story"]["video"]["videoAuthorId"]
        .. "&count=20"
        .. "&offset="
      )
      local largest_size = nil
      local largest_url = nil
      for video_id, video_data in pairs(json["story"]["video"]["videoList"]) do
        local size = tonumber(string.match(video_data["definition"], "^([0-9]+)"))
        if not largest_size or size > largest_size then
          if video_data["backup_url_1"] ~= video_data["main_url"] then
            error("Found conflict in main and backup video URLs.")
          end
          largest_size = size
          largest_url = video_data["main_url"]
        end
      end
      if not largest_url then
        error("Could not find a video.")
      end
      if string.match(largest_url, "^https?:(.+)$") ~= json["story"]["video"]["videoUrl"] then
        error("Found different video URLs.")
      end
      local newurl = urlparse.absolute(url, json["story"]["video"]["videoUrl"])
      if json["story"]["videoUrl"] ~= newurl then
        error("Inconsistent video URLs.")
      end
      force_queue(newurl)
      force_queue(json["story"]["imgUrl"])
      force_queue("https://p16-va.topbuzzcdn.com/list/" .. json["story"]["video"]["videoThumbnail"]["web_uri"])
    end
    if string.match(url, "/api/1200/web/comment_v2/comments") then
      local json = JSON:decode(html)
      for _, comment_data in pairs(json["data"]["data"]) do
        if tonumber(comment_data["reply_count"]) > 0 then
          check(
            "https://www.buzzvideo.com/api/1200/web/comment_v2/detail"
            .. "?" .. string.gsub(api_params_string, "(&aid=)[0-9]+", "%16816")
            .. "&item_id=" .. string.match(url, "&item_id=([0-9]+)")
            .. "&group_id=" .. string.match(url, "&group_id=([0-9]+)")
            .. "&media_id=" .. string.match(url, "&media_id=([0-9]+)")
            .. "&count=10"
            .. "&offset=0"
            .. "&comment_id=" .. comment_data["id"]
          )
        end
        if comment_data["image_list"] then
          for _, image_data in pairs(comment_data["image_list"]) do
            for _, key in pairs({"thumb_url_list", "url_list"}) do
              for _, url_data in pairs(image_data[key]) do
                force_queue(url_data["url"])
              end
            end
          end
        end
      end
    end
    if string.match(url, "/api/1200/web/comment_v2") then
      local json = JSON:decode(html)
      discover_from_json(json)
      if json["message"] ~= "success" then
        error("Got bad data from API.")
      end
      local has_more_key = nil
      local cursor_key = nil
      if string.match(url, "comment_v2/detail") then
        has_more_key = "reply_has_more"
        cursor_key = "reply_cursor"
      elseif string.match(url, "comment_v2/comments") then
        has_more_key = "has_more"
        cursor_key = "cursor"
      else
        error("Could not find cursor keys.")
      end
      if json["data"][has_more_key] then
        local newurl = string.gsub(url, "(&offset=)[0-9]*", "%1" .. tostring(json["data"][cursor_key]))
        check(newurl)
      end
    end
    if string.match(url, "/api/777/web/stream") then
      local json = JSON:decode(html)
      discover_from_json(json)
      if json["message"] ~= "success" then
        error("Got bad data from API.")
      end
      if json["data"]["has_more"] and web_stream_count < 10 then
        local final_item = nil
        for _, item_data in pairs(json["data"]["items"]) do
          final_item = item_data
        end
        if not final_item then
          error("No item found in stream API data.")
        end
        local newurl = url
        newurl = string.gsub(newurl, "(&max_behot_time=)[0-9%.]+", "%1" .. tostring(final_item["behot_time"]))
        newurl = string.gsub(newurl, "(&session_impr_id=)[0-9]+", "%1" .. tostring(final_item["impr_id"]))
        check(newurl)
        web_stream_count = web_stream_count + 1
      end
    end
    --[[for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end]]
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  find_item(url["url"])
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  find_item(url["url"])

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    --[[if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end]]
    if string.match(url["url"], "^https?://www%.buzzvideo%.com/a/[0-9]+$")
      and string.match(newloc, "^https?://[^/]+/@.*%-[0-9a-zA-Z_]+$") then
      ids[string.match(newloc, "%-([0-9a-zA-Z_]+)$")] = true
      return wget.actions.NOTHING
    end
    if string.match(url["url"], "^https?://www%.buzzvideo%.com/article/i[0-9]+$") then
      return wget.actions.NOTHING
    end
    error("Unexpected redirect.")
  end
  
  if status_code == 200 then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if (status_code == 0 or status_code >= 400)
    and status_code ~= 404 then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 3
    tries = tries + 1
    if tries > maxtries then
      tries = 0
      abort_item()
      return wget.actions.ABORT
    end
    os.execute("sleep " .. math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    ))
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 4
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and tonumber(JSON:decode(body)["status_code"]) == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["buzzvideo-dm1qvscd9n8yttj"] = discovered_items,
    ["urls-0c8gj3ckiphl42s"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

