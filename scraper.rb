require 'scraperwiki'
require 'mechanize'
require 'nokogiri'
require 'active_support'
require 'active_support/core_ext'
require './petitions'

Time.zone = 'Brisbane'

URI_PAGE_LIST = 'http://www.parliament.qld.gov.au/work-of-assembly/petitions/e-petitions'
URI_FRAME_LIST = 'https://www.parliament.qld.gov.au/apps/Epetitions/CurrentEPetitions.aspx'
URI_PAGE_ITEM = 'http://www.parliament.qld.gov.au/work-of-assembly/petitions/e-petition?PetNum=%{petition_num}'
URI_FRAME_ITEM = 'https://www.parliament.qld.gov.au/apps/Epetitions/CurrentEPetition.aspx?PetNum=%{petition_num}'

petitions_helper = Petitions.new
current_time = Time.zone.now

# Get and save petitions
petitions_hash = []
open(URI_FRAME_LIST) do |i|
  petitions_page = i.read
  petitions_hash = petitions_helper.get_petitions(petitions_page)
end

petitions_hash.each do |petition|
  uri = URI_FRAME_ITEM % {petition_num: petition[:reference_num]}
  new_hash = {
      retrieved_at: current_time,
      url: uri
  }

  open(uri) do |i|
    petition_page = i.read
    petition_hash = petitions_helper.get_item(petition_page)

    new_hash.merge!(petition).merge!(petition_hash)

  end

  ScraperWiki.save_sqlite([:reference_num, :signatures], new_hash, 'data')
end
