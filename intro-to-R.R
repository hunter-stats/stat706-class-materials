# These are comments

# R/IDE Overview
# load faraway package (install if missing with install.packages)
install.packages('faraway')
library(faraway)

# load gavote data
data("gavote")

# look at help for gavote
?gavote
# look at the summary
summary(gavote)

# add a new variable (undercount (see page 3 of book))
gavote$votes

gavote$undercount <- (gavote$ballots - gavote$votes)/gavote$ballots

# histogram of undercount (load ggplot2)
library(ggplot2)
ggplot(gavote, aes(x = undercount)) +
  geom_histogram()

summary(gavote$undercount)

# density of undercount with rug
ggplot(gavote, aes(x = undercount)) +
  geom_density() +
  geom_rug(sides = "b", alpha = .5)

# Frequencies of equipment ( `equip`)
xtabs(~equip + rural, data=  gavote)
# percent gore (gore/votes)
gavote$perc_gore <- gavote$gore/gavote$votes

ggplot(gavote, aes(x = perc_gore, y = perAA)) +
  geom_point() +
  geom_smooth(alpha = .2)

# Convert this to an R Markdown file
