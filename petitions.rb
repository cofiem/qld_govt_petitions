class Petitions

  # Get petitions from list page
  # @param [String] page
  # @return [Hash]
  def get_petitions(page)
    html = Nokogiri::HTML(page)
    table = html.css('table[border="1"]')
    trs = table.css('tr')

    items = []

    if trs.size > 1
      trs.drop(1).each do |tr|
        item = parse_list_row(tr)
        items.push(item) unless item.nil?
      end
    end

    items
  end

  # Get petition from item page
  # @param [String] page
  # @return [Hash]
  def get_item(page)
    html = Nokogiri::HTML(page)
    table = html.css('table[cellpadding="2"]')
    trs = table.css('tr')

    nbsp = Nokogiri::HTML('&nbsp;').text

    item = {
        body: html.css('table.petitionDescription').text.gsub(nbsp, ' ').to_s.squish,
        principal: ''
    }

    if trs.size > 0
      trs.each do |tr|
        col1 = tr.css('td')[0].text.to_s
        col2 = tr.css('td')[1].text.to_s.strip
        case col1
          when 'Subject:'
            item[:subject] = col2
          when 'Eligibility:'
            item[:eligibility] = col2
          when 'Sponsoring Member:'
            item[:sponsor] = col2
          when 'Principal Petitioner:'
            item[:principal] += "#{col2.squish}\n"
          when 'Number of Signatures:'
            item[:signatures] = col2.to_i
          when 'Posting Date:'
            item[:posted_at] = col2
          when 'Closing Date:'
            item[:closed_at] = col2
          else
            item[:principal] += "#{col2.squish}\n"
        end
      end
    end

    as = html.css('a')
    a = as.select { |a| a['href'].starts_with?('mailto') }.first
    a_value = a['href'].scan(/PetNum=(\d+)/).first.first
    item[:reference_num] = a_value.to_i

    item[:addressed_to] = html.css('#ContentPlaceHolder1_Label2').text.to_s.gsub('TO: ', '')

    item[:principal] = item[:principal].strip

    item
  end

  private

  def parse_list_row(html)
    tds = html.css('td')
    if tds.size < 1
      # no petitions, nothing to do here
      nil
    else
      ref_name = tds[0].text
      ref_num = ref_name.split('-')[0].to_i
      {
          reference_name: ref_name,
          reference_num: ref_num,
          subject: tds[1].text,
          signatures: tds[2].text.to_i,
          closed_at: tds[3].text + ' 00:00:00 +10:00'
      }
    end
  end
end